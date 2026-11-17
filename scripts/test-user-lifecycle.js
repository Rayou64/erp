const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const vm = require('node:vm');
const { test } = require('node:test');
const { createDbClient } = require('../db/client');
const { USER_RESTORE_WINDOW_MS, ensureProjectAssignmentRetention, purgeExpiredUserAccounts } = require('../lib/user-lifecycle');

test('PostgreSQL retention migration quotes existing mixed-case constraint names exactly once', async () => {
  const source = fs.readFileSync(path.join(__dirname, '..', 'db', 'client.js'), 'utf8');
  const context = vm.createContext();
  vm.runInContext(source.slice(source.indexOf('function quoteCamelCaseIdentifiers('), source.indexOf('function createSqliteClient(')), context);
  const statements = [];
  await ensureProjectAssignmentRetention({
    driver: 'postgres',
    all: async () => [{ name: 'project_assignments_userId_fkey' }],
    run: async sql => statements.push(context.quoteCamelCaseIdentifiers(sql)),
  });
  assert.match(statements[0], /DROP CONSTRAINT "project_assignments_userId_fkey"/);
  assert.match(statements[0], /ALTER COLUMN "userId" DROP NOT NULL/);
  assert.match(statements[0], /REFERENCES users\(id\) ON DELETE SET NULL/);
  assert.doesNotMatch(statements[0], /""/);
  assert.equal(context.quoteCamelCaseIdentifiers(`SELECT userId, 'mixedCase', "alreadyQuoted" FROM users`),
    `SELECT "userId", 'mixedCase', "alreadyQuoted" FROM users`);
});

test('existing SQLite assignment schemas retain history, indexes and triggers when a user is removed', async () => {
  const db = createDbClient({ driver: 'sqlite', dbFile: ':memory:' });
  try {
    await db.run('CREATE TABLE users (id INTEGER PRIMARY KEY)');
    await db.run(`CREATE TABLE project_assignments (
      id INTEGER PRIMARY KEY, userId INTEGER NOT NULL, assigneeName TEXT,
      FOREIGN KEY(userId) REFERENCES users(id) ON DELETE CASCADE
    )`);
    await db.run('CREATE INDEX assignment_user ON project_assignments(userId)');
    await db.run('CREATE TABLE assignment_audit (id INTEGER)');
    await db.run(`CREATE TRIGGER assignment_update AFTER UPDATE ON project_assignments
      BEGIN INSERT INTO assignment_audit (id) VALUES (NEW.id); END`);
    await db.run('INSERT INTO users VALUES (1)');
    await db.run("INSERT INTO project_assignments VALUES (1, 1, 'Employé de validation')");
    await ensureProjectAssignmentRetention(db);
    await ensureProjectAssignmentRetention(db);
    await db.run('DELETE FROM users WHERE id = 1');
    assert.deepEqual(await db.get('SELECT * FROM project_assignments'), { id: 1, userId: null, assigneeName: 'Employé de validation' });
    assert.ok(await db.get("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'assignment_user'"));
    assert.ok(await db.get('SELECT id FROM assignment_audit WHERE id = 1'));
  } finally {
    await db.close();
  }
});

test('profiles, employee names and deletion survive actual API calls and server restart', { timeout: 240000 }, async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'erp-user-lifecycle-'));
  const dbFile = path.join(directory, 'test.db');
  const socket = net.createServer();
  socket.listen(0, '127.0.0.1');
  await once(socket, 'listening');
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  let child;
  let output = '';
  let db;
  let adminToken;
  t.after(async () => {
    if (db) await db.close();
    if (child && child.exitCode === null) {
      const exited = once(child, 'exit');
      child.kill();
      await exited;
    }
    fs.rmSync(directory, { recursive: true, force: true });
  });

  async function start() {
    child = spawn(process.execPath, [path.join(__dirname, '..', 'app.js')], {
      env: {
        ...process.env, APP_DATA_DIR: directory, DB_FILE: dbFile, DATABASE_DRIVER: 'sqlite', DATABASE_URL: '',
        ARCHIVE_ROOT: path.join(directory, 'archives'), NODE_ENV: 'test', PORT: String(port), LOCALHOST_FALLBACK_PORT: '0',
        AUTO_BACKUP_ENABLED: '0', PROD_SYNC_URL: '', GPS_RETENTION_DAYS: '0',
        FIREBASE_SERVICE_ACCOUNT_JSON: '', FIREBASE_SERVICE_ACCOUNT_FILE: '',
        JWT_SECRET: 'isolated-test-key-not-a-production-secret',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    const deadline = Date.now() + 90000;
    while (Date.now() < deadline) {
      try {
        if ((await fetch(base + '/readyz')).ok) return;
      } catch (error) {
        if (error.cause?.code !== 'ECONNREFUSED' && error.cause?.code !== 'UND_ERR_SOCKET') throw error;
      }
      if (child.exitCode !== null) throw new Error(output);
      await new Promise(resolve => setTimeout(resolve, 150));
    }
    throw new Error('ERP startup failed: ' + output);
  }

  async function api(url, { method = 'GET', body, token = adminToken, expected = 200 } = {}) {
    const response = await fetch(base + url, {
      method,
      headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const result = await response.json();
    assert.equal(response.status, expected, JSON.stringify(result));
    return result;
  }

  db = createDbClient({ driver: 'sqlite', dbFile });
  await db.run(`CREATE TABLE native_push_subscriptions (
    token TEXT PRIMARY KEY, userId INTEGER NOT NULL, appId TEXT NOT NULL, updatedAt TEXT NOT NULL
  )`);
  await db.close();
  db = null;
  await start();
  db = createDbClient({ driver: 'sqlite', dbFile });
  adminToken = (await api('/api/auth/login', { method: 'POST', token: '', body: { username: 'admin', password: 'admin123' } })).token;
  const user = await db.get("SELECT id FROM users WHERE username = 'commis_stock'");
  assert.equal((await api('/api/push/native/status')).enabled, false);
  await api('/api/push/native/subscribe', { method: 'POST', body: { token: 'bad', appId: 'com.ryan.erp' }, expected: 400 });
  await api('/api/push/native/subscribe', {
    method: 'POST', body: { token: 'a'.repeat(100), appId: 'com.ryan.erp', notificationVersion: 99 }, expected: 400,
  });
  await api('/api/push/native/subscribe', {
    method: 'POST', body: { token: 'a'.repeat(100), appId: 'com.ryan.portailsafari' }, expected: 503,
  });
  await db.run(`INSERT INTO native_push_subscriptions (token, userId, appId, updatedAt) VALUES (?, ?, ?, ?)`,
    ['isolated-device-token', user.id, 'com.ryan.portailsafari', new Date().toISOString()]);
  assert.equal((await db.get('SELECT notificationVersion FROM native_push_subscriptions WHERE token = ?', ['isolated-device-token'])).notificationVersion, 0);
  const recipient = await db.get("SELECT id FROM users WHERE username = 'achat'");
  const before = new Date(Date.now() - 1000).toISOString();
  await db.run("INSERT INTO projects (id, nomProjet, nomSite, createdAt) VALUES (9000, 'Projet de validation', 'Site de validation', ?)", [before]);
  await db.run(`INSERT INTO project_assignments (id, projectId, userId, assigneeName, role, assignedAt)
    VALUES (9000, 9000, ?, 'Employé de validation', 'Commis', ?)`, [user.id, before]);
  await db.run(`INSERT INTO hr_employees (id, fullName, username, jobTitle, createdAt, updatedAt)
    VALUES (9000, 'Employé de validation', 'commis_stock', 'Commis', ?, ?)`, [before, before]);
  await db.run(`INSERT INTO hr_employees (id, fullName, username, jobTitle, createdAt, updatedAt)
    VALUES (9001, 'Commis Stock', 'commis_stock', 'Commis', ?, ?)`, [before, before]);
  await db.run(`INSERT INTO hr_employees (id, fullName, username, jobTitle, createdAt, updatedAt)
    VALUES (9002, 'Commis Stock', 'commis_stock', 'Commis', ?, ?)`, [before, before]);
  const senderToken = (await api('/api/auth/login', { method: 'POST', token: '', body: { username: 'commis_stock', password: 'stock123' } })).token;
  const receiverToken = (await api('/api/auth/login', { method: 'POST', token: '', body: { username: 'achat', password: 'achat123' } })).token;
  const directoryRows = await api('/api/internal-mail/users', { token: senderToken });
  assert.equal(directoryRows.find(row => row.id === user.id).fullName, 'Employé de validation');
  const conversation = await api('/api/internal-mail/conversations', {
    method: 'POST', token: senderToken, expected: 201,
    body: { recipientIds: [recipient.id], subject: 'Validation', body: 'Message de test' },
  });
  assert.equal((await api('/api/internal-mail/conversations', { token: receiverToken }))[0].senderName, 'Employé de validation');
  assert.equal((await api('/api/internal-mail/unread-count', { token: receiverToken })).items[0].senderName, 'Employé de validation');
  await api(`/api/admin/users/${user.id}`, { method: 'DELETE', token: senderToken, expected: 403 });
  const admin = await db.get("SELECT id FROM users WHERE username = 'admin'");
  await api(`/api/admin/users/${admin.id}`, { method: 'DELETE', expected: 409 });
  await api(`/api/admin/users/${user.id}/deactivate`, { method: 'PATCH', token: senderToken, expected: 403 });
  await api(`/api/admin/users/${admin.id}/deactivate`, { method: 'PATCH', expected: 409 });
  const duplicates = (await api('/api/users')).filter(row => row.id === user.id);
  assert.deepEqual(duplicates.map(row => row.linkedEmployeeId).sort(), [9000, 9001, 9002]);
  assert.ok(duplicates.every(row => row.linkedProfileCount === 3));
  await api(`/api/admin/users/${user.id}/deactivate`, { method: 'PATCH', body: { employeeId: 9002 } });
  const partiallyDisabled = (await api('/api/users')).filter(row => row.id === user.id);
  assert.ok(partiallyDisabled.find(row => row.linkedEmployeeId === 9002).deactivatedAt);
  assert.ok(!partiallyDisabled.find(row => row.linkedEmployeeId === 9000).deactivatedAt);
  await api('/api/auth/me', { token: senderToken });
  assert.ok(await db.get('SELECT token FROM native_push_subscriptions WHERE userId = ?', [user.id]), 'keep device access while another linked profile is active');
  await api(`/api/admin/users/${user.id}/restore`, { method: 'PATCH', body: { employeeId: 9002 } });
  await api(`/api/admin/users/${user.id}/deactivate`, { method: 'PATCH', body: { employeeId: 99999 }, expected: 404 });
  await api(`/api/admin/users/${user.id}`, { method: 'DELETE', body: { employeeId: 9002 }, expected: 400 });
  await api(`/api/admin/users/${user.id}`, { method: 'DELETE', body: { employeeId: 9002, confirm: 'DELETE_USER_PERMANENTLY' } });
  await api(`/api/admin/users/${user.id}/restore`, { method: 'PATCH', body: { employeeId: 9002 }, expected: 404 });
  assert.ok(await db.get('SELECT id FROM hr_employees WHERE id = 9002'), 'keep the HR dossier');
  const remainingDuplicates = (await api('/api/users')).filter(row => row.id === user.id);
  assert.deepEqual(remainingDuplicates.map(row => row.linkedEmployeeId).sort(), [9000, 9001]);
  await api('/api/admin/reconcile-hr-users', { method: 'POST', body: {} });
  assert.equal((await api('/api/users')).filter(row => row.id === user.id).length, 2, 'do not recreate a removed duplicate');
  await api('/api/auth/me', { token: senderToken });
  const response = await api(`/api/admin/users/${user.id}/deactivate`, { method: 'PATCH' });
  const disabled = await db.get('SELECT deactivatedAt, deleteAfter FROM users WHERE id = ?', [user.id]);
  assert.equal(await db.get('SELECT token FROM native_push_subscriptions WHERE userId = ?', [user.id]), undefined);
  assert.equal(Date.parse(disabled.deleteAfter) - Date.parse(disabled.deactivatedAt), USER_RESTORE_WINDOW_MS);
  assert.deepEqual(await api(`/api/admin/users/${user.id}/deactivate`, { method: 'PATCH' }), response);
  assert.deepEqual(await db.get('SELECT deactivatedAt, deleteAfter FROM users WHERE id = ?', [user.id]), disabled);
  await api('/api/auth/me', { token: senderToken, expected: 401 });
  await api('/api/auth/profile-stream?mobileAuth=' + senderToken, { expected: 401 });
  await api('/api/push/public-key', { token: senderToken, expected: 401 });
  await api('/api/auth/login', { method: 'POST', token: '', body: { username: 'commis_stock', password: 'stock123' }, expected: 403 });
  assert.ok(!(await api('/api/internal-mail/users')).some(row => row.id === user.id));
  await api('/api/internal-mail/conversations', { method: 'POST', expected: 400, body: { recipientIds: [user.id], subject: 'Refus', body: 'Test' } });
  const disabledRows = await api('/api/users');
  assert.equal(disabledRows.filter(row => row.id === user.id).length, 2);
  assert.ok(disabledRows.find(row => row.id === user.id).deactivatedAt);
  await api(`/api/admin/users/${user.id}/restore`, { method: 'PATCH' });
  await api('/api/auth/me', { token: senderToken, expected: 401 });
  await api('/api/auth/login', { method: 'POST', token: '', body: { username: 'commis_stock', password: 'stock123' } });
  await api(`/api/admin/users/${user.id}/deactivate`, { method: 'PATCH' });
  const boundary = new Date().toISOString();
  await db.run('UPDATE users SET deleteAfter = ? WHERE id = ?', [boundary, user.id]);
  await db.run('UPDATE user_profile_states SET deleteAfter = ? WHERE userId = ? AND deletedAt = ?', [boundary, user.id, '']);
  await purgeExpiredUserAccounts(db, new Date(Date.parse(boundary) - 1).toISOString());
  assert.ok(await db.get('SELECT id FROM users WHERE id = ?', [user.id]), 'retain the profile until the exact deadline');
  await api(`/api/admin/users/${user.id}/restore`, { method: 'PATCH', expected: 410 });
  await purgeExpiredUserAccounts(db, boundary);
  assert.equal(await db.get('SELECT id FROM users WHERE id = ?', [user.id]), undefined);
  assert.equal((await db.get('SELECT userId FROM project_assignments WHERE id = 9000')).userId, null);
  assert.ok((await api('/api/project-assignments')).some(row => row.id === 9000));
  assert.ok(await db.get('SELECT userId FROM deleted_user_accounts WHERE userId = ?', [user.id]));
  assert.equal((await api(`/api/internal-mail/conversations/${conversation.id}`, { token: receiverToken })).messages[0].senderName, 'Employé de validation');
  assert.ok(!(await api('/api/users')).some(row => row.username === 'commis_stock'));
  await api('/api/admin/reconcile-hr-users', { method: 'POST', body: {} });
  assert.equal(await db.get("SELECT id FROM users WHERE username = 'commis_stock'"), undefined);
  await api('/api/push/test', { method: 'POST', expected: 409, body: {} });
  for (const [id, deactivate] of [[9500, false], [9501, true]]) {
    const username = 'delete_validation_' + id;
    await db.run(`INSERT INTO users (id, username, password, role, createdAt)
      SELECT ?, ?, password, 'employe_standard', ? FROM users WHERE id = ?`, [id, username, before, recipient.id]);
    if (deactivate) await api(`/api/admin/users/${id}/deactivate`, { method: 'PATCH' });
    await api(`/api/admin/users/${id}`, { method: 'DELETE', body: { confirm: 'DELETE_USER_PERMANENTLY' } });
    assert.equal(await db.get('SELECT id FROM users WHERE id = ?', [id]), undefined);
    await api(`/api/admin/users/${id}/restore`, { method: 'PATCH', expected: 404 });
    await api('/api/auth/login', { method: 'POST', token: '', body: { username, password: 'achat123' }, expected: 401 });
  }
  for (const [index, username] of ['gestionnaire_stock', 'chef_adzope_site15', 'gest_zone_adzope'].entries()) {
    await db.run(`INSERT INTO users (id, username, password, role, createdAt)
      SELECT ?, ?, password, 'gestionnaire_stock', ? FROM users WHERE id = ?`, [9600 + index, username, before, recipient.id]);
  }
  for (const id of [9700, 9701]) {
    await db.run(`INSERT INTO hr_employees (id, fullName, username, createdAt, updatedAt)
      VALUES (?, 'Profil existant à conserver', 'gestionnaire_stock', ?, ?)`, [id, before, before]);
  }
  const exited = once(child, 'exit');
  child.kill();
  await exited;
  await start();
  assert.equal(await db.get("SELECT id FROM users WHERE username = 'commis_stock'"), undefined, 'seed must not recreate deleted accounts');
  assert.equal((await db.get('SELECT fullName FROM hr_employees WHERE id = 9000')).fullName, 'Employé de validation');
  for (const id of [9600, 9601, 9602]) {
    const preserved = await db.get('SELECT id, deactivatedAt, deleteAfter FROM users WHERE id = ?', [id]);
    assert.ok(preserved, 'restarting must never remove an existing legacy profile');
    assert.equal(preserved.deactivatedAt, '');
    assert.equal(preserved.deleteAfter, '');
  }
  assert.equal((await api('/api/users')).filter(row => row.id === 9600).length, 2, 'existing duplicates stay visible after restart');
  assert.ok(!output.includes('Erreur d\'initialisation'), output);
});
