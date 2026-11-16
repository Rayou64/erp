const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { test } = require('node:test');
const { createNativePush, validNativeSubscription } = require('../lib/native-push');
const root = path.join(__dirname, '..');

test('native subscriptions accept only supported Android applications and well-formed tokens', () => {
  for (const appId of ['com.ryan.erp', 'com.ryan.portailsafari']) {
    assert.ok(validNativeSubscription('device:token_with-at-least-32-characters', appId));
  }
  assert.equal(validNativeSubscription('short', 'com.ryan.erp'), false);
  assert.equal(validNativeSubscription('a'.repeat(100), 'other.app'), false);
  assert.equal(validNativeSubscription('https://untrusted.example/' + 'a'.repeat(100), 'com.ryan.erp'), false);
});

test('native delivery includes sound, employee title, subject/preview and conversation deep link; expired devices are removed', async () => {
  const removed = [];
  const messages = [];
  const push = createNativePush({
    all: async (sql, params) => {
      assert.match(sql, /u.deactivatedAt = ''/);
      assert.deepEqual(params, [12]);
      return [{ token: 'valid-device', appId: 'com.ryan.portailsafari' }, { token: 'expired-device', appId: 'com.ryan.erp' }];
    },
    run: async (_sql, params) => removed.push(params[0]),
    messaging: { sendEachForMulticast: async message => {
      messages.push(message);
      return { responses: [{ success: true }, { success: false, error: { code: 'messaging/registration-token-not-registered' } }] };
    } },
  });
  assert.equal(await push.send([12, 12], {
    title: 'Nouveau message de Employé Test', message: 'Réunion\nBonjour', url: '/messaging.html?conversation=8', tag: 'internal-mail:9',
  }), 1);
  assert.deepEqual(removed, ['expired-device']);
  assert.equal(messages[0].notification.title, 'Nouveau message de Employé Test');
  assert.equal(messages[0].notification.body, 'Réunion\nBonjour');
  assert.equal(messages[0].android.notification.channelId, 'internal_mail');
  assert.equal(messages[0].android.notification.sound, 'default');
  assert.equal(messages[0].android.notification.visibility, 'public');
  assert.equal(messages[0].data.url, '/messaging.html?conversation=8');
});

test('native test targets only the authenticated user device and unavailable Firebase fails explicitly', async () => {
  const push = createNativePush({
    env: {},
    all: async (sql, params) => {
      assert.match(sql, /s.token = \?/);
      assert.deepEqual(params, [3, 'device-token']);
      return [{ token: 'device-token' }];
    },
    run: async () => {},
  });
  assert.equal(push.enabled, false);
  await assert.rejects(push.send([3], {}, 'device-token'), /non configuré/);
});

test('one failed Firebase batch does not prevent attempts to later recipient devices', async () => {
  const batches = [];
  const errors = [];
  const push = createNativePush({
    all: async () => Array.from({ length: 501 }, (_, index) => ({ token: 'device-' + index, appId: 'com.ryan.erp' })),
    run: async () => {},
    logger: { error: (...args) => errors.push(args) },
    messaging: { sendEachForMulticast: async message => {
      batches.push(message.tokens.length);
      if (batches.length === 1) throw new Error('First batch failed');
      return { responses: [{ success: true }] };
    } },
  });
  await assert.rejects(push.send([1], { title: 'Test' }), /First batch failed/);
  assert.deepEqual(batches, [500, 1]);
  assert.equal(errors.length, 1);
});

function nativeClient({ permission = 'granted', enabled = true, registrationError = false, plugins = true, subscribe = null } = {}) {
  const listeners = new Map();
  const events = [];
  const values = new Map([['erp_token', 'session-user-a']]);
  const push = {
    addListener: async (name, handler) => listeners.set(name, handler),
    checkPermissions: async () => ({ receive: permission }),
    requestPermissions: async () => { events.push('permission'); permission = 'granted'; return { receive: 'granted' }; },
    createChannel: async channel => events.push(channel),
    register: async () => {
      if (registrationError) listeners.get('registrationError')({ error: 'Firebase unavailable' });
      else await listeners.get('registration')({ value: 'a'.repeat(100) });
    },
    unregister: async () => events.push('unregister'),
  };
  const local = {
    addListener: async (name, handler) => listeners.set(name, handler),
    schedule: async data => events.push(data),
    removeAllDeliveredNotifications: async () => events.push('clear-notifications'),
  };
  const context = vm.createContext({
    setTimeout, clearTimeout, Event: class {}, CustomEvent: class {},
    localStorage: { getItem: key => values.get(key), setItem: (key, value) => values.set(key, value), removeItem: key => values.delete(key) },
    window: {
      ERPMobile: { native: true, appId: 'com.ryan.portailsafari', openNotification: url => events.push(url) },
      Capacitor: { Plugins: plugins ? { PushNotifications: push, LocalNotifications: local } : {} },
      dispatchEvent: () => {},
    },
    console: { error() {} },
    fetch: async (url, options) => {
      events.push({ url, auth: options.headers.Authorization, ...(options.body ? JSON.parse(options.body) : {}) });
      if (url === '/api/push/native/subscribe' && subscribe) await subscribe();
      return { ok: true, json: async () => ({ enabled }) };
    },
  });
  vm.runInContext(fs.readFileSync(path.join(root, 'public', 'native-mail-notifications.js'), 'utf8'), context);
  return { api: context.window.ERPNativePush, listeners, events, values, setPermission: value => { permission = value; } };
}

test('native plugin registers the correct app, creates a sound-enabled high-priority channel, and tests this device only', async () => {
  const client = nativeClient({ permission: 'prompt' });
  await client.api.enable({ requestPermission: true });
  assert.ok(client.events.includes('permission'));
  const channel = client.events.find(event => event?.id === 'internal_mail');
  assert.equal(channel.importance, 5);
  assert.equal(channel.vibration, true);
  assert.equal(channel.visibility, 1);
  const subscribe = client.events.find(event => event?.url === '/api/push/native/subscribe');
  assert.equal(subscribe.appId, 'com.ryan.portailsafari');
  await client.api.test();
  assert.equal(client.events.find(event => event?.url === '/api/push/native/test').token, subscribe.token);
  await client.api.unsubscribe();
  assert.ok(client.events.includes('unregister'));
  assert.ok(client.events.includes('clear-notifications'));
  assert.equal(client.values.get('erp_native_push_token'), undefined);
});

test('foreground native messages appear in device notifications and native taps use the conversation link', async () => {
  const client = nativeClient();
  await client.api.enable();
  await client.listeners.get('pushNotificationReceived')({
    title: 'Employé Test', body: 'Objet\nAperçu', data: { url: '/messaging.html?conversation=17' },
  });
  const notification = client.events.find(event => event?.notifications)?.notifications[0];
  assert.equal(notification.channelId, 'internal_mail');
  assert.equal(notification.body, 'Objet\nAperçu');
  client.listeners.get('pushNotificationActionPerformed')({ notification: { data: { url: '/messaging.html?conversation=17' } } });
  client.listeners.get('localNotificationActionPerformed')({ notification: { extra: notification.extra } });
  assert.equal(client.events.filter(event => event === '/messaging.html?conversation=17').length, 2);
});

test('revoked Android permission and logout stop foreground system notifications', async () => {
  const client = nativeClient();
  await client.api.enable();
  client.setPermission('denied');
  await client.listeners.get('pushNotificationReceived')({ title: 'Message', body: 'Test' });
  assert.equal(client.events.some(event => event?.notifications), false);
  await client.api.unsubscribe();
  client.setPermission('granted');
  await client.listeners.get('pushNotificationReceived')({ title: 'Message', body: 'Test' });
  assert.equal(client.events.some(event => event?.notifications), false);
});

test('native registration cannot start without an authenticated account', async () => {
  const client = nativeClient();
  client.values.delete('erp_token');
  await assert.rejects(client.api.enable(), /Connectez-vous/);
  assert.equal(client.events.some(event => event?.url === '/api/push/native/subscribe'), false);
});

test('a late registration is cancelled and removed with its original account after logout', async () => {
  let release;
  let began;
  const started = new Promise(resolve => { began = resolve; });
  const client = nativeClient({ subscribe: () => { began(); return new Promise(resolve => { release = resolve; }); } });
  const operation = client.api.enable();
  const cancelled = assert.rejects(operation, error => error.code === 'NATIVE_PUSH_CANCELLED');
  await started;
  await client.api.unsubscribe();
  client.values.delete('erp_token');
  release();
  await cancelled;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(client.values.get('erp_native_push_token'), undefined);
  const cleanup = client.events.find(event => event?.url === '/api/push/native/unsubscribe');
  assert.equal(cleanup.auth, 'Bearer session-user-a');
});

test('a replaced registration cannot delete the newer binding for the same account', async () => {
  let release;
  let began;
  let attempts = 0;
  const started = new Promise(resolve => { began = resolve; });
  const client = nativeClient({
    subscribe: async () => {
      attempts += 1;
      if (attempts === 1) {
        began();
        await new Promise(resolve => { release = resolve; });
      }
    },
  });
  const previous = client.api.enable();
  const cancelled = assert.rejects(previous, error => error.code === 'NATIVE_PUSH_CANCELLED');
  await started;
  await client.api.enable();
  release();
  await cancelled;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(client.events.some(event => event?.url === '/api/push/native/unsubscribe'), false);
  assert.equal(client.values.get('erp_native_push_token'), 'a'.repeat(100));
});

test('native permissions, missing configuration, outdated APK and Firebase errors cannot report success', async () => {
  await assert.rejects(nativeClient({ permission: 'denied' }).api.enable(), /Autorisez/);
  await assert.rejects(nativeClient({ enabled: false }).api.enable(), /configuré/);
  await assert.rejects(nativeClient({ registrationError: true }).api.enable(), /Firebase unavailable/);
  await assert.rejects(nativeClient({ plugins: false }).api.enable(), /Mettez à jour/);
});

function mobileEntry({ native = false, mail = false, search = '', token = '', pending = null } = {}) {
  const session = new Map(pending ? [['erp_pending_mail_url', pending]] : []);
  const navigations = [];
  const classes = new Set();
  const context = vm.createContext({
    URL, URLSearchParams,
    document: { documentElement: { classList: { add: name => classes.add(name) } } },
    sessionStorage: { getItem: key => session.get(key), setItem: (key, value) => session.set(key, value), removeItem: key => session.delete(key) },
    localStorage: { getItem: () => token },
    location: { origin: 'https://erp.example', pathname: '/erp.html', search, assign: url => navigations.push(url) },
    window: { Capacitor: { isNativePlatform: () => native }, RyanNative: { isMessagingOnly: () => mail } },
  });
  vm.runInContext(fs.readFileSync(path.join(root, 'public', 'mobile-messaging.js'), 'utf8'), context);
  return { api: context.window.ERPMobile, session, navigations, classes };
}

test('Portail Safari and optional web mail login open the same inbox; normal RyanERP entry is unchanged', () => {
  assert.equal(mobileEntry().api.destination(), '/erp.html');
  const safari = mobileEntry({ native: true, mail: true });
  assert.equal(safari.api.appId, 'com.ryan.portailsafari');
  assert.equal(safari.api.destination(), '/messaging.html');
  assert.ok(safari.classes.has('messaging-only-app'));
  assert.equal(mobileEntry({ native: true }).classes.has('messaging-only-app'), false);
  assert.equal(mobileEntry({ mail: true }).api.messagingOnly, true, 'the native identity is available before Capacitor JavaScript');
  assert.equal(mobileEntry({ mail: true }).api.native, true);
  assert.equal(mobileEntry({ search: '?mail=1' }).api.destination(), '/messaging.html');
  assert.equal(mobileEntry({ native: true }).api.appId, 'com.ryan.erp');
});

test('notification conversation survives login, unsafe URLs are rejected, and signed-in users open it directly', () => {
  const guest = mobileEntry();
  guest.api.openNotification('/messaging.html?conversation=42');
  assert.equal(guest.api.destination(), '/messaging.html?conversation=42');
  assert.deepEqual(guest.navigations, ['/index.html?mail=1']);
  const signedIn = mobileEntry({ token: 'test-session' });
  signedIn.api.openNotification('/messaging.html?conversation=42');
  assert.deepEqual(signedIn.navigations, ['/messaging.html?conversation=42']);
  assert.throws(() => signedIn.api.openNotification('https://other.example/messaging.html'), /invalide/);
  assert.throws(() => signedIn.api.openNotification('/messaging.html?conversation=javascript:alert(1)'), /invalide/);
  signedIn.api.clearPending();
  assert.equal(signedIn.api.destination(), '/erp.html');
});

test('RyanERP home links directly to the existing mailbox, without installing Portail Safari or entering Admin', () => {
  const html = fs.readFileSync(path.join(root, 'public', 'erp.html'), 'utf8');
  const home = html.slice(html.indexOf('<div id="mobile-premium-home"'), html.indexOf('<div id="dashboard-classic-grid"'));
  assert.match(home, /id="mph-mail-shortcut" href="\/messaging.html"/);
  assert.match(home, /Messagerie%20interne.png/);
  assert.match(home, /id="mph-mail-count"/);
  for (const file of ['erp.html', 'index.html', 'messaging.html']) {
    const source = fs.readFileSync(path.join(root, 'public', file), 'utf8');
    assert.ok(source.indexOf('mobile-messaging.js') < source.indexOf('native-mail-notifications.js'));
    for (const match of source.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi)) {
      new vm.Script(match[1], { filename: file });
    }
  }
});

test('published ERP update matches its APK and existing download links; optional Safari APK is available', () => {
  const manifest = JSON.parse(fs.readFileSync(path.join(root, 'public', 'mobile-version.json'), 'utf8'));
  const apk = fs.readFileSync(path.join(root, 'public', 'app-release.apk'));
  assert.equal(createHash('sha256').update(apk).digest('hex'), manifest.sha256);
  assert.deepEqual(fs.readFileSync(path.join(root, 'public', 'downloads', 'app-release.apk')), apk);
  assert.equal(manifest.apkUrl, 'https://ryanerp-hn5zd.ondigitalocean.app/app-release.apk');
  const gradle = fs.readFileSync(path.join(root, 'mobile', 'android', 'app', 'build.gradle'), 'utf8');
  assert.match(gradle, new RegExp('\\bversionCode\\s+' + manifest.versionCode + '\\b'));
  assert.ok(gradle.includes('versionName "' + manifest.versionName + '"'));
  const safari = fs.statSync(path.join(root, 'public', 'portail-safari.apk'));
  assert.ok(safari.isFile() && safari.size > 0);
});
