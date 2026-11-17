const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { test } = require('node:test');
const { createDbClient } = require('../db/client');
const vm = require('node:vm');

test('a failed mail transaction cannot roll back another request on the shared SQLite connection', async () => {
  const db = createDbClient({ driver: 'sqlite', dbFile: ':memory:' });
  try {
    await db.run('CREATE TABLE mail_test (id INTEGER PRIMARY KEY)');
    let entered, release;
    const started = new Promise(resolve => { entered = resolve; });
    const resume = new Promise(resolve => { release = resolve; });
    const failed = db.transaction(async tx => {
      await tx.run('INSERT INTO mail_test VALUES (1)');
      entered();
      await resume;
      throw new Error('Rejected mail write');
    });

    const rejected = assert.rejects(failed, /Rejected mail write/);
    await started;
    const other = db.run('INSERT INTO mail_test VALUES (2)');
    release();
    await Promise.all([rejected, other]);
    assert.deepEqual(await db.all('SELECT id FROM mail_test'), [{ id: 2 }]);
  } finally {
    await db.close();
  }
});

test('PostgreSQL transactions keep every statement, rollback and release on their reserved connection', async () => {
  const statements = [];
  let releases = 0;
  const connection = {
    query: async (sql, params) => { statements.push({ sql, params }); return { rows: [{ id: 7 }], rowCount: 1 }; },
    release: () => { releases++; },
  };
  class Pool {
    async connect() { return connection; }
    query() { throw new Error('Transaction escaped to the pool'); }
    async end() {}
  }
  const context = vm.createContext({
    require: name => name === 'pg' ? { Pool } : require(name),
    process: { env: { PGSSL_DISABLE: '1' } }, URL, module: { exports: {} },
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'db', 'client.js'), 'utf8'), context);
  const db = context.module.exports.createDbClient({ driver: 'postgres', databaseUrl: 'postgres://test.invalid/test', dbFile: ':memory:' });
  const result = await db.transaction(async tx => {
    await tx.run('INSERT INTO mail_test (conversationId) VALUES (?)', [7]);
    return (await tx.get('SELECT conversationId FROM mail_test WHERE id = ?', [7])).id;
  });
  assert.equal(result, 7);
  assert.equal(statements[0].sql, 'BEGIN');
  assert.equal(statements[1].sql, 'INSERT INTO mail_test ("conversationId") VALUES ($1)');
  assert.equal(statements.at(-1).sql, 'COMMIT');
  await assert.rejects(db.transaction(async tx => {
    await tx.all('SELECT conversationId FROM mail_test');
    throw new Error('Rejected transaction');
  }), /Rejected transaction/);
  assert.equal(statements.at(-1).sql, 'ROLLBACK');
  assert.equal(releases, 2);
});

test('mailbox workflows use persisted, account-scoped messages', { timeout: 180000 }, async t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'erp-mail-workflows-'));
  const socket = net.createServer();
  socket.listen(0, '127.0.0.1');
  await once(socket, 'listening');
  const port = socket.address().port;
  await new Promise(resolve => socket.close(resolve));
  const base = `http://127.0.0.1:${port}`;
  let output = '';
  const child = spawn(process.execPath, [path.join(__dirname, '..', 'app.js')], {
    env: {
      ...process.env, APP_DATA_DIR: directory, DB_FILE: path.join(directory, 'test.db'),
      DATABASE_DRIVER: 'sqlite', DATABASE_URL: '', ARCHIVE_ROOT: path.join(directory, 'archives'),
      NODE_ENV: 'test', PORT: String(port), LOCALHOST_FALLBACK_PORT: '0',
      AUTO_BACKUP_ENABLED: '0', PROD_SYNC_URL: '', GPS_RETENTION_DAYS: '0',
      FIREBASE_SERVICE_ACCOUNT_JSON: '', FIREBASE_SERVICE_ACCOUNT_FILE: '',
      HR_DIRECTOR_PASSWORD: 'isolated-hr-mail-test-password',
      JWT_SECRET: 'isolated-mail-test-key-not-a-production-secret',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout.on('data', chunk => { output += chunk; });
  child.stderr.on('data', chunk => { output += chunk; });
  t.after(async () => {
    if (child.exitCode === null) {
      const exited = once(child, 'exit');
      child.kill();
      await exited;
    }
    fs.rmSync(directory, { recursive: true, force: true });
  });
  const deadline = Date.now() + 90000;
  let ready = false;
  while (Date.now() < deadline) {
    try {
      if ((await fetch(base + '/readyz')).ok) { ready = true; break; }
    } catch (error) {
      if (!['ECONNREFUSED', 'UND_ERR_SOCKET'].includes(error.cause?.code)) throw error;
    }
    if (child.exitCode !== null) throw new Error(output);
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  assert.ok(ready, 'Server did not start: ' + output);
  let token = '';
  async function api(url, { method = 'GET', body, expected = 200, token: requestToken = token } = {}) {
    const response = await fetch(base + url, {
      method,
      headers: { 'Content-Type': 'application/json', ...(requestToken ? { Authorization: 'Bearer ' + requestToken } : {}) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const payload = await response.json();
    assert.equal(response.status, expected, JSON.stringify(payload) + (response.status >= 500 ? '\n' + output.slice(-2500) : ''));
    return payload;
  }
  token = (await api('/api/auth/login', {
    method: 'POST', body: { username: 'admin', password: 'admin123' },
  })).token;
  const users = await api('/api/internal-mail/users');
  const me = users.find(user => user.username === 'admin');
  assert.ok(me?.id);
  const stock = users.find(user => user.username === 'commis_stock');
  const buyer = users.find(user => user.username === 'achat');
  const login = async (username, password) => (await api('/api/auth/login', {
    method: 'POST', token: '', body: { username, password },
  })).token;
  const stockToken = await login('commis_stock', 'stock123');
  const buyerToken = await login('achat', 'achat123');
  const hrToken = await login('directeur_rh', 'isolated-hr-mail-test-password');
  async function employee(username, fullName) {
    return api('/api/hr/employees', {
      method: 'POST', expected: 201,
      body: { username, fullName, jobTitle: 'Validation', dateEmbauche: '2026-01-01', createUserAccount: 'false' },
    });
  }
  const adminEmployee = await employee('admin', 'Administrateur de validation');
  const stockEmployee = await employee('commis_stock', 'Employe Stock Validation');
  const buyerEmployee = await employee('achat', 'Employe Achats Validation');
  const unlinkedEmployee = await employee('without_login', 'Employe sans compte');
  let team;
  let historical;

  await t.test('an explicit self-recipient receives the sent message in the inbox', async () => {
    const created = await api('/api/internal-mail/conversations', {
      method: 'POST', expected: 201,
      body: { recipientIds: [me.id], subject: 'Suivi de validation', body: 'Message adresse explicitement a soi-meme.' },
    });
    assert.ok((await api('/api/internal-mail/conversations?folder=inbox')).some(row => row.id === created.id));
    assert.ok((await api('/api/internal-mail/conversations?folder=sent')).some(row => row.id === created.id));
    const detail = await api('/api/internal-mail/conversations/' + created.id);
    assert.equal(detail.messages[0].recipients[0].id, me.id);
  });

  await t.test('a draft is not delivered to its selected recipients', async () => {
    const recipient = users.find(user => user.username === 'achat');
    assert.ok(recipient?.id);
    const created = await api('/api/internal-mail/conversations', {
      method: 'POST', expected: 201,
      body: {
        status: 'draft', recipientIds: [recipient.id], subject: 'Pas encore envoye', body: 'Brouillon prive.',
        attachments: [{ fileName: 'brouillon.txt', contentBase64: Buffer.from('Prive').toString('base64') }],
      },
    });
    assert.ok((await api('/api/internal-mail/conversations?folder=drafts')).some(row => row.id === created.id));
    assert.ok(!(await api('/api/internal-mail/conversations?folder=inbox')).some(row => row.id === created.id));
    const detail = await api('/api/internal-mail/conversations/' + created.id);
    assert.equal(detail.messages[0].recipients[0].id, recipient.id, 'retain intended recipients for resuming the draft');
    const recipientToken = (await api('/api/auth/login', {
      method: 'POST', token: '', body: { username: 'achat', password: 'achat123' },
    })).token;
    assert.ok(!(await api('/api/internal-mail/conversations?folder=inbox', { token: recipientToken })).some(row => row.id === created.id));
    assert.equal((await api('/api/internal-mail/unread-count', { token: recipientToken })).count, 0);
    await api('/api/internal-mail/conversations/' + created.id, { token: recipientToken, expected: 404 });
    await api('/api/internal-mail/attachments/' + detail.messages[0].attachments[0].id + '/download', { token: recipientToken, expected: 404 });
  });

  await t.test('only admin and HR manage real employee teams; members and recipients are deduplicated', async () => {
    await api('/api/hr/teams', { token: stockToken, expected: 403 });
    await api('/api/hr/teams', { method: 'POST', token: buyerToken, body: { name: 'Refuse', employeeIds: [] }, expected: 403 });
    team = await api('/api/hr/teams', {
      method: 'POST', expected: 201,
      body: { name: 'RH validation', employeeIds: [adminEmployee.id, stockEmployee.id, stockEmployee.id, unlinkedEmployee.id] },
    });
    assert.equal(team.members.length, 3);
    assert.equal(team.recipientCount, 2);
    assert.equal(team.isMember, true);
    assert.equal(team.members.find(person => person.employeeId === unlinkedEmployee.id).userId, null);
    await api('/api/hr/teams', { method: 'POST', expected: 409, body: { name: 'RH VALIDATION', employeeIds: [] } });
    await api('/api/hr/teams/' + team.id, { method: 'PATCH', expected: 400, body: { name: team.name, employeeIds: [999999] } });
    assert.equal((await api('/api/hr/teams')).find(item => item.id === team.id).members.length, 3);
    await api('/api/hr/teams/' + team.id, {
      method: 'PATCH', token: hrToken,
      body: { name: team.name, employeeIds: team.employeeIds },
    });
    historical = await api('/api/internal-mail/conversations', {
      method: 'POST', expected: 201,
      body: {
        subject: 'Historique RH', body: 'Conversation avant adhesion', teamIds: [team.id],
        recipientIds: [stock.id], ccRecipientIds: [stock.id],
        attachments: [{ fileName: 'historique.txt', contentBase64: Buffer.from('Historique de validation').toString('base64') }],
      },
    });
    const recipients = (await api('/api/internal-mail/conversations/' + historical.id)).messages[0].recipients;
    assert.equal(recipients.filter(person => person.id === stock.id).length, 1);
    assert.equal(recipients.length, 2);
    assert.ok(!(await api('/api/internal-mail/teams', { token: buyerToken })).find(item => item.id === team.id).isMember);
    await api('/api/internal-mail/conversations?teamId=' + team.id, { token: buyerToken, expected: 403 });
    await api('/api/internal-mail/conversations/' + historical.id, { token: buyerToken, expected: 404 });
  });

  await t.test('joining a team exposes its full history and attachments without rewriting old recipients or sharing private mail', async () => {
    const privateMail = await api('/api/internal-mail/conversations', {
      method: 'POST', expected: 201, body: { recipientIds: [stock.id], subject: 'Prive', body: 'Non partage avec RH' },
    });
    team = await api('/api/hr/teams/' + team.id, {
      method: 'PATCH', body: { name: team.name, employeeIds: [...team.employeeIds, buyerEmployee.id] },
    });
    assert.ok((await api('/api/internal-mail/teams', { token: buyerToken })).find(item => item.id === team.id).isMember);
    assert.ok((await api('/api/internal-mail/conversations?teamId=' + team.id, { token: buyerToken })).some(row => row.id === historical.id));
    const detail = await api('/api/internal-mail/conversations/' + historical.id, { token: buyerToken });
    assert.ok(!detail.messages[0].recipients.some(person => person.id === buyer.id), 'preserve the original recipient snapshot');
    const attachment = await fetch(base + '/api/internal-mail/attachments/' + detail.messages[0].attachments[0].id + '/download', {
      headers: { Authorization: 'Bearer ' + buyerToken },
    });
    assert.equal(attachment.status, 200);
    assert.equal(await attachment.text(), 'Historique de validation');
    await api('/api/internal-mail/conversations/' + privateMail.id, { token: buyerToken, expected: 404 });
    await api('/api/internal-mail/conversations/' + historical.id + '/replies', {
      method: 'POST', token: buyerToken, expected: 201, body: { mode: 'reply-all', body: 'Reponse a toute equipe' },
    });
    const privateReply = await api('/api/internal-mail/conversations/' + historical.id + '/replies', {
      method: 'POST', token: stockToken, expected: 201, body: { mode: 'reply', body: 'Reponse privee' },
    });
    assert.notEqual(privateReply.conversationId, historical.id);
    await api('/api/internal-mail/conversations/' + privateReply.conversationId, { expected: 404 });
    assert.equal((await api('/api/internal-mail/conversations/' + historical.id)).messages.length, 2);
    team = await api('/api/hr/teams/' + team.id, {
      method: 'PATCH', body: { name: team.name, employeeIds: team.employeeIds.filter(id => id !== buyerEmployee.id) },
    });
    await api('/api/internal-mail/conversations?teamId=' + team.id, { token: buyerToken, expected: 403 });
  });

  await t.test('draft updates, retries and final sending preserve one conversation and its files', async () => {
    const payload = {
      status: 'draft', draftKey: 'workflow-draft', subject: 'Brouillon stable', body: 'Version 1',
      teamIds: [team.id], bccRecipientIds: [buyer.id],
      attachments: [{ fileName: 'conserve.txt', contentBase64: Buffer.from('A conserver').toString('base64') }],
    };
    const [first, retry] = await Promise.all([
      api('/api/internal-mail/conversations', { method: 'POST', body: payload, expected: 201 }),
      api('/api/internal-mail/conversations', { method: 'POST', body: payload, expected: 201 }),
    ]);
    assert.equal(first.id, retry.id);
    const revised = { ...payload, body: 'Version 2' };
    const revision = await api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', body: revised });
    await api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', body: { ...revised, body: 'Modification obsolette', expectedUpdatedAt: first.updatedAt }, expected: 409 });
    await api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', token: stockToken, body: revised, expected: 404 });
    await api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', body: { ...revised, projectId: 999999 }, expected: 400 });
    const reloaded = await api('/api/internal-mail/conversations/' + first.id);
    assert.equal(reloaded.messages[0].body, 'Version 2');
    assert.equal(Buffer.from(reloaded.messages[0].attachments[0].contentBase64, 'base64').toString(), 'A conserver');
    assert.equal(reloaded.messages[0].recipients.find(person => person.id === buyer.id).recipientType, 'bcc');
    await api('/api/internal-mail/conversations/' + first.id, { token: buyerToken, expected: 404 });
    const sent = { ...revised, status: 'sent' };
    await Promise.all([
      api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', body: sent }),
      api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', body: sent }),
    ]);
    await api('/api/internal-mail/conversations/' + first.id + '/draft', { method: 'PUT', body: { ...sent, body: 'Nouveau contenu apres envoi', expectedUpdatedAt: revision.updatedAt }, expected: 409 });
    assert.ok(!(await api('/api/internal-mail/conversations?folder=drafts')).some(row => row.id === first.id));
    assert.ok((await api('/api/internal-mail/conversations', { token: buyerToken })).some(row => row.id === first.id));
    const received = await api('/api/internal-mail/conversations/' + first.id, { token: stockToken });
    assert.equal(received.messages.length, 1);
    assert.equal(received.messages[0].attachments.length, 1);
    assert.ok(!received.messages[0].recipients.some(person => person.id === buyer.id), 'keep Bcc private from other recipients');
  });

  await t.test('trash and restore are per account and preserve source folders, flags and private drafts', async () => {
    await api('/api/internal-mail/conversations/' + historical.id, { method: 'PATCH', body: { action: 'trash', sourceFolder: 'sent' } });
    assert.ok(!(await api('/api/internal-mail/conversations?folder=sent')).some(row => row.id === historical.id));
    assert.ok((await api('/api/internal-mail/conversations?folder=trash')).some(row => row.id === historical.id));
    assert.ok((await api('/api/internal-mail/conversations', { token: stockToken })).some(row => row.id === historical.id));
    const restored = await api('/api/internal-mail/conversations/' + historical.id, { method: 'PATCH', body: { action: 'restore' } });
    assert.equal(restored.restoredFolder, 'sent');
    await api('/api/internal-mail/conversations/' + historical.id, { method: 'PATCH', token: stockToken, body: { action: 'star' } });
    await api('/api/internal-mail/conversations/' + historical.id, { method: 'PATCH', token: stockToken, body: { action: 'trash', sourceFolder: 'inbox' } });
    assert.ok(!(await api('/api/internal-mail/conversations?folder=starred', { token: stockToken })).some(row => row.id === historical.id));
    const recipientRestore = await api('/api/internal-mail/conversations/' + historical.id, { method: 'PATCH', token: stockToken, body: { action: 'restore' } });
    assert.equal(recipientRestore.restoredFolder, 'inbox');
    assert.ok((await api('/api/internal-mail/conversations?folder=starred', { token: stockToken })).some(row => row.id === historical.id));
    const draft = await api('/api/internal-mail/conversations', {
      method: 'POST', expected: 201, body: { status: 'draft', subject: 'Corbeille brouillon', body: 'A reprendre' },
    });
    await api('/api/internal-mail/conversations/' + draft.id, { method: 'PATCH', body: { action: 'trash', sourceFolder: 'drafts' } });
    await api('/api/internal-mail/conversations/' + draft.id + '/draft', { method: 'PUT', body: { status: 'draft', body: 'Ne pas ecraser' }, expected: 409 });
    assert.ok(!(await api('/api/internal-mail/conversations?folder=drafts')).some(row => row.id === draft.id));
    assert.equal((await api('/api/internal-mail/conversations/' + draft.id, { method: 'PATCH', body: { action: 'restore' } })).restoredFolder, 'drafts');
    assert.equal((await api('/api/internal-mail/conversations/' + draft.id)).messages[0].body, 'A reprendre');
  });
});
