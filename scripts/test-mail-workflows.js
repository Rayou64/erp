const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { test } = require('node:test');

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
    assert.equal(response.status, expected, JSON.stringify(payload));
    return payload;
  }
  token = (await api('/api/auth/login', {
    method: 'POST', body: { username: 'admin', password: 'admin123' },
  })).token;
  const users = await api('/api/internal-mail/users');
  const me = users.find(user => user.username === 'admin');
  assert.ok(me?.id);

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
});
