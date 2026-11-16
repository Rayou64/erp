const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const server = fs.readFileSync(path.join(__dirname, '..', 'app.js'), 'utf8');
const client = fs.readFileSync(path.join(__dirname, '..', 'public', 'push-notifications.js'), 'utf8');

function pushServer({ subscriptions, send }) {
  const deliveries = [];
  const deletions = [];
  const errors = [];
  const context = vm.createContext({
    pushVapid: { publicKey: 'test' },
    nativePush: { send: async () => 0 },
    all: async (sql, params) => {
      assert.match(sql, /u.deactivatedAt = ''/);
      return subscriptions(params[0]);
    },
    run: async (_sql, params) => { deletions.push(params[0]); },
    getInternalMailUser: async () => ({ fullName: 'Employé de validation', username: 'profil_test' }),
    webpush: { sendNotification: async (subscription, payload, options) => {
      deliveries.push({ subscription, payload: JSON.parse(payload), options });
      if (send) await send(subscription);
    } },
    console: { error: (...args) => errors.push(args) },
  });
  vm.runInContext(server.slice(server.indexOf('async function sendPushToUsers('), server.indexOf("app.get('/api/push/public-key'")), context);
  return { context, deliveries, deletions, errors };
}

test('new mail notifies every recipient device using the linked employee name, excluding sender', async () => {
  const harness = pushServer({
    subscriptions: id => [{ id, endpoint: `https://push.example.test/${id}/phone`, p256dh: 'key', auth: 'auth' },
      { id: id + 10, endpoint: `https://push.example.test/${id}/tablet`, p256dh: 'key', auth: 'auth' }],
  });
  await harness.context.notifyInternalMailRecipients(1, [1, 2, 2, 3], 12, 34, 'Courriel de validation', 'Texte');
  assert.equal(harness.deliveries.length, 4);
  for (const delivery of harness.deliveries) {
    assert.equal(delivery.payload.title, 'Nouveau message de Employé de validation');
    assert.equal(delivery.payload.url, '/messaging.html?conversation=12');
    assert.equal(delivery.payload.tag, 'internal-mail:34');
    assert.equal(delivery.payload.message, 'Courriel de validation\nTexte');
    assert.equal(delivery.options.TTL, 86400);
    assert.equal(delivery.options.urgency, 'high');
  }
});

test('expired subscriptions are removed and failed delivery does not skip other devices', async () => {
  const harness = pushServer({
    subscriptions: id => [{ id, endpoint: String(id) }],
    send: async subscription => {
      if (subscription.endpoint === '1') throw Object.assign(new Error('Expired'), { statusCode: 410 });
      if (subscription.endpoint === '2') throw Object.assign(new Error('Unavailable'), { statusCode: 503 });
    },
  });
  await assert.rejects(harness.context.sendPushToUsers([1, 2, 3], { title: 'Test' }), /Unavailable/);
  assert.deepEqual(harness.deletions, [1]);
  assert.equal(harness.deliveries.length, 3);
  assert.equal(harness.errors.length, 1);
  const empty = pushServer({ subscriptions: () => [] });
  assert.equal(await empty.context.sendPushToUsers([1], {}), 0);
});

function pushBrowser({ ios = false, standalone = false, denied = false, failure = '', existing = null } = {}) {
  const events = [];
  const elements = new Map([
    ['[data-push-status]', { textContent: '' }],
    ['[data-push-enable]', { addEventListener() {} }],
    ['[data-push-test]', { addEventListener() {} }],
  ]);
  const panel = { style: {}, setAttribute() {}, querySelector: selector => elements.get(selector) };
  let load;
  const registration = {
    pushManager: {
      getSubscription: async () => existing,
      subscribe: async () => {
        events.push('subscribe');
        return { endpoint: 'https://push.example.test/phone', keys: { auth: 'test', p256dh: 'test' } };
      },
    },
  };
  const context = vm.createContext({
    navigator: {
      userAgent: ios ? 'iPhone' : 'Android', platform: '', maxTouchPoints: 1,
      serviceWorker: {
        register: async () => { events.push('register'); return registration; },
        ready: Promise.resolve(registration), addEventListener() {},
      },
    },
    Notification: {
      permission: denied ? 'denied' : 'default',
      requestPermission: () => { events.push('permission'); return Promise.resolve('granted'); },
    },
    localStorage: { getItem: () => 'test-token' },
    atob: value => Buffer.from(value, 'base64').toString('binary'),
    document: {
      readyState: 'loading',
      addEventListener: (_event, handler) => { load = handler; },
      querySelector: () => ({ appendChild() {} }), createElement: () => panel,
    },
    window: {
      isSecureContext: true, matchMedia: () => ({ matches: standalone }),
      PushManager: function () {}, Notification: {}, addEventListener() {}, dispatchEvent() {},
    },
    console: { error: () => {} },
    fetch: async (url, options) => {
      events.push(url);
      if (options.body) events.push(JSON.parse(options.body));
      return {
        ok: !failure,
        json: async () => failure ? { error: failure } : { enabled: true, publicKey: 'AQID' },
      };
    },
  });
  vm.runInContext(client, context);
  load();
  return { api: context.window.ERPPush, events, status: elements.get('[data-push-status]') };
}

test('mobile subscription requests permission from the click before awaiting network or service worker', async () => {
  const browser = pushBrowser();
  await browser.api.enable();
  assert.equal(browser.events.length, 0, 'no permission prompt without a user gesture');
  assert.equal(await browser.api.enable({ requestPermission: true }), true);
  assert.equal(browser.events[0], 'permission');
  assert.ok(browser.events.includes('register'));
  assert.ok(browser.events.includes('subscribe'));
  assert.ok(browser.events.includes('/api/push/subscribe'));
  assert.match(browser.status.textContent, /activées/);
});

test('iPhone explains installation, standalone iPhone subscribes, and failures stay visible', async () => {
  const safari = pushBrowser({ ios: true });
  assert.equal(await safari.api.enable({ requestPermission: true }), false);
  assert.match(safari.status.textContent, /Sur l’écran d’accueil/);
  assert.equal(safari.events.length, 0);
  const installed = pushBrowser({ ios: true, standalone: true });
  await installed.api.enable();
  assert.equal(await installed.api.enable({ requestPermission: true }), true);
  const blocked = pushBrowser({ denied: true });
  assert.equal(await blocked.api.enable({ requestPermission: true }), false);
  assert.match(blocked.status.textContent, /bloquées/);
  const failed = pushBrowser({ failure: 'Serveur indisponible' });
  await failed.api.enable();
  assert.equal(await failed.api.enable({ requestPermission: true }), false);
  assert.match(failed.status.textContent, /Serveur indisponible/);
});

test('rotated VAPID keys replace the old device subscription before registration', async () => {
  let unsubscribed = false;
  const browser = pushBrowser({
    existing: {
      options: { applicationServerKey: Uint8Array.from([9, 8, 7]).buffer },
      unsubscribe: async () => { unsubscribed = true; },
    },
  });
  await browser.api.enable();
  assert.equal(await browser.api.enable({ requestPermission: true }), true);
  assert.ok(unsubscribed);
  assert.ok(browser.events.includes('subscribe'));
});

test('service worker displays and opens actual mail notifications with or without an existing window', async () => {
  const listeners = {};
  const shown = [];
  const opened = [];
  const context = vm.createContext({
    self: {
      addEventListener: (name, handler) => { listeners[name] = handler; },
      clients: { matchAll: async () => [], openWindow: async url => { opened.push(url); } },
      registration: { showNotification: async (title, options) => { shown.push({ title, options }); } },
    },
    URL,
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'public', 'sw.js'), 'utf8'), context);
  let completed;
  const payload = { title: 'Courriel', message: 'Test', module: 'internal-mail', url: '/messaging.html?conversation=12', tag: 'internal-mail:34' };
  listeners.push({ data: { json: () => payload }, waitUntil: promise => { completed = promise; } });
  await completed;
  assert.equal(shown[0].options.tag, payload.tag);
  assert.equal(shown[0].options.data.url, payload.url);
  listeners.notificationclick({ notification: { close() {}, data: shown[0].options.data }, waitUntil: promise => { completed = promise; } });
  await completed;
  assert.deepEqual(opened, [payload.url]);
  let navigated;
  context.self.clients.matchAll = async () => [{ url: 'https://erp.example.test/erp.html', focus: async () => {}, navigate: async url => { navigated = url; } }];
  listeners.notificationclick({ notification: { close() {}, data: shown[0].options.data }, waitUntil: promise => { completed = promise; } });
  await completed;
  assert.equal(navigated, payload.url);
  context.self.clients.matchAll = async () => [{ url: 'https://erp.example.test/messaging.html', focus: async () => {}, navigate: async url => { navigated = url; } }];
  navigated = null;
  listeners.notificationclick({ notification: { close() {}, data: shown[0].options.data }, waitUntil: promise => { completed = promise; } });
  await completed;
  assert.equal(navigated, payload.url, 'reuse an already open mailbox');
  for (const file of ['erp.html', 'messaging.html', 'index.html']) {
    const html = fs.readFileSync(path.join(__dirname, '..', 'public', file), 'utf8');
    assert.match(html, /rel="manifest"/);
    for (const match of html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)) new vm.Script(match[1], { filename: file });
  }
});
