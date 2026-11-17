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

function pushBrowser({ ios = false, standalone = false, denied = false, permission = denied ? 'denied' : 'default', failure = '', existing = null, nativePush = null, auth = 'test-token' } = {}) {
  const events = [];
  const notices = [];
  const states = [];
  const requests = [];
  const values = new Map([['erp_token', auth]]);
  const listeners = new Map();
  const documentListeners = new Map();
  let subscription = existing;
  const registration = {
    pushManager: {
      getSubscription: async () => subscription,
      subscribe: async () => {
        events.push('subscribe');
        subscription = {
          endpoint: 'https://push.example.test/phone', keys: { auth: 'test', p256dh: 'test' },
          unsubscribe: async () => { events.push('unsubscribe'); subscription = null; },
        };
        return subscription;
      },
    },
  };
  const context = vm.createContext({
    navigator: {
      userAgent: ios ? 'iPhone' : 'Android', platform: '', maxTouchPoints: 1,
      serviceWorker: {
        register: async () => { events.push('register'); return registration; },
        getRegistration: async () => registration,
        ready: Promise.resolve(registration), addEventListener() {},
      },
    },
    Notification: {
      permission,
      requestPermission: () => {
        events.push('permission');
        context.Notification.permission = 'granted';
        return Promise.resolve('granted');
      },
    },
    CustomEvent: class { constructor(type, options = {}) { this.type = type; this.detail = options.detail; } },
    Event: class { constructor(type) { this.type = type; } },
    localStorage: {
      getItem: key => values.get(key) ?? null,
      setItem: (key, value) => values.set(key, value),
      removeItem: key => values.delete(key),
    },
    atob: value => Buffer.from(value, 'base64').toString('binary'),
    document: {
      readyState: 'complete', hidden: false,
      addEventListener: (event, handler) => documentListeners.set(event, handler),
      createElement: () => { throw new Error('The activation banner must not be created'); },
    },
    window: {
      isSecureContext: true, matchMedia: () => ({ matches: standalone }),
      PushManager: function () {}, Notification: {},
      ERPNativePush: nativePush,
      Capacitor: { isNativePlatform: () => Boolean(nativePush) },
      addEventListener: (event, handler) => listeners.set(event, handler),
      dispatchEvent: event => {
        if (event.type === 'erp-push-status') states.push(event.detail);
        listeners.get(event.type)?.(event);
      },
      showMessage: (message, type) => notices.push({ message, type }),
      alert: message => notices.push({ message, type: 'alert' }),
    },
    console: { error: () => {} },
    fetch: async (url, options) => {
      events.push(url);
      requests.push({ url, auth: options.headers.Authorization });
      if (options.body) events.push(JSON.parse(options.body));
      return {
        ok: !failure,
        json: async () => failure ? { error: failure } : { enabled: true, publicKey: 'AQID' },
      };
    },
  });
  vm.runInContext(client, context);
  return {
    api: context.window.ERPPush, events, notices, states, requests, values,
    get status() { return context.window.ERPPush.getStatus(); },
    emit: (name, data = {}) => listeners.get(name)?.(data),
    emitDocument: name => documentListeners.get(name)?.(),
  };
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
  assert.match(browser.status.message, /activées/);
});

test('iPhone explains installation, standalone iPhone subscribes, and failures stay visible', async () => {
  const safari = pushBrowser({ ios: true });
  assert.equal(await safari.api.enable({ requestPermission: true }), false);
  assert.match(safari.status.message, /Sur l’écran d’accueil/);
  assert.equal(safari.events.length, 0);
  const installed = pushBrowser({ ios: true, standalone: true });
  await installed.api.enable();
  assert.equal(await installed.api.enable({ requestPermission: true }), true);
  const blocked = pushBrowser({ denied: true });
  assert.equal(await blocked.api.enable({ requestPermission: true }), false);
  assert.match(blocked.status.message, /bloquées/);
  const failed = pushBrowser({ failure: 'Serveur indisponible' });
  await failed.api.enable();
  assert.equal(await failed.api.enable({ requestPermission: true }), false);
  assert.match(failed.status.message, /Serveur indisponible/);
  assert.ok(failed.notices.some(notice => notice.message.includes('Serveur indisponible')));
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

test('granted browser notifications subscribe automatically without an activation banner or permission prompt', async () => {
  const browser = pushBrowser({ permission: 'granted' });
  assert.equal(await browser.api.enable(), true);
  assert.ok(browser.events.includes('/api/push/subscribe'));
  assert.equal(browser.events.includes('permission'), false);
  assert.equal(browser.status.enabled, true);
  assert.equal(browser.notices.length, 0);
});

test('native alerts wait for Android consent and automatically register after the system permission callback', async () => {
  let permission = 'prompt';
  const actions = [];
  const nativePush = {
    checkPermission: async () => permission,
    enable: async options => { actions.push(options); },
  };
  const browser = pushBrowser({ nativePush });
  assert.equal(await browser.api.enable(), false);
  assert.equal(actions.length, 0);
  assert.equal(browser.notices.length, 0);
  permission = 'granted';
  browser.emit('erp-native-notification-permission');
  assert.equal(await browser.api.enable(), true);
  assert.equal(actions.length, 1);
  assert.equal(actions[0].requestPermission, undefined);
  assert.equal(browser.status.enabled, true);
  assert.equal(browser.notices.length, 0);
});

test('automatic registration requires a signed-in account and follows later authentication', async () => {
  const browser = pushBrowser({ permission: 'granted', auth: null });
  assert.equal(await browser.api.enable(), false);
  assert.equal(browser.events.length, 0);
  browser.values.set('erp_token', 'authenticated-user');
  browser.emit('storage', { key: 'erp_token' });
  assert.equal(await browser.api.enable(), true);
  assert.ok(browser.requests.every(request => request.auth === 'Bearer authenticated-user'));
});

test('denied Android permission never registers and only gives one temporary settings warning', async () => {
  const browser = pushBrowser({
    nativePush: {
      checkPermission: async () => 'denied',
      enable: async () => { throw new Error('Registration must not run without consent'); },
    },
  });
  assert.equal(await browser.api.enable(), false);
  browser.emit('focus');
  assert.equal(await browser.api.enable(), false);
  assert.equal(browser.notices.length, 1);
  assert.equal(browser.notices[0].type, 'warning');
  assert.match(browser.notices[0].message, /réglages Android/);
});

test('focus and resume cannot re-enable alerts after logout, even if an old registration finishes late', async () => {
  let finish;
  let began;
  const started = new Promise(resolve => { began = resolve; });
  const nativePush = {
    checkPermission: async () => 'granted',
    enable: () => { began(); return new Promise(resolve => { finish = resolve; }); },
    unsubscribe: async () => {},
  };
  const browser = pushBrowser({ nativePush });
  await started;
  const operation = browser.api.enable();
  await browser.api.unsubscribe();
  browser.values.delete('erp_token');
  finish();
  assert.equal(await operation, false);
  browser.emit('focus');
  browser.emit('erp-native-notification-permission');
  assert.equal(await browser.api.enable(), false);
  assert.equal(browser.status.enabled, false);
});

test('an account change during registration is automatically rebound to the new account', async () => {
  let finish;
  let began;
  const accounts = [];
  const started = new Promise(resolve => { began = resolve; });
  const browser = pushBrowser({
    nativePush: {
      checkPermission: async () => 'granted',
      enable: () => {
        accounts.push(browser.values.get('erp_token'));
        if (accounts.length > 1) return Promise.resolve();
        began();
        return new Promise(resolve => { finish = resolve; });
      },
    },
  });
  await started;
  const operation = browser.api.enable();
  browser.values.set('erp_token', 'session-user-b');
  browser.emit('storage', { key: 'erp_token' });
  finish();
  await operation;
  assert.equal(await browser.api.enable(), true);
  assert.deepEqual(accounts, ['test-token', 'session-user-b']);
});

test('native activation failures remain explicit without displaying a permanent activation panel', async () => {
  const browser = pushBrowser({
    nativePush: {
      checkPermission: async () => 'granted',
      enable: async () => { throw new Error('Firebase indisponible'); },
    },
  });
  assert.equal(await browser.api.enable(), false);
  assert.equal(browser.status.enabled, false);
  assert.ok(browser.notices.some(notice => notice.type === 'error' && notice.message.includes('Firebase indisponible')));
  for (const file of ['erp.html', 'messaging.html']) {
    const source = fs.readFileSync(path.join(__dirname, '..', 'public', file), 'utf8');
    assert.doesNotMatch(source, /data-push-settings-host/);
  }
  assert.doesNotMatch(client, /data-push-enable|Activer les alertes courriel|createElement\('section'\)/);
});

test('restoring the network retries native activation automatically', async () => {
  let online = false;
  const browser = pushBrowser({
    nativePush: {
      checkPermission: async () => 'granted',
      enable: async () => { if (!online) throw new Error('Connexion indisponible'); },
    },
  });
  assert.equal(await browser.api.enable(), false);
  online = true;
  browser.emit('online');
  assert.equal(await browser.api.enable(), true);
  assert.equal(browser.notices.length, 1, 'successful automatic recovery does not display an activation message');
});

test('mailbox mobile styles update from the network even when an older style is cached', async () => {
  const listeners = {};
  const fresh = { version: 'current-styles' };
  let cacheReads = 0;
  const context = vm.createContext({
    self: { location: { origin: 'https://erp.example.test' }, addEventListener: (name, handler) => { listeners[name] = handler; } },
    caches: { match: async () => { cacheReads++; return { version: 'old-styles' }; } },
    fetch: async () => fresh,
    URL,
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '..', 'public', 'sw.js'), 'utf8'), context);
  let response;
  listeners.fetch({
    request: { method: 'GET', url: 'https://erp.example.test/messaging-mobile.css?v=2' },
    respondWith: promise => { response = promise; },
  });
  assert.equal(await response, fresh);
  assert.equal(cacheReads, 0);
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
