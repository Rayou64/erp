(function () {
  'use strict';
  if (!window.ERPMobile?.native) return;
  const push = window.Capacitor?.Plugins?.PushNotifications;
  const local = window.Capacitor?.Plugins?.LocalNotifications;
  const appId = window.ERPMobile.appId;
  const tokenKey = 'erp_native_push_token';
  const channelId = 'internal_mail';
  let listening;
  let resolveRegistration;
  let rejectRegistration;
  let registrationAuth = '';
  let registrationRevision = 0;
  let boundAuth = '';

  async function api(path, body, auth = localStorage.getItem('erp_token')) {
    const response = await fetch(path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: 'Bearer ' + auth, 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'Service Android indisponible.');
    return result;
  }

  function report(error) {
    console.error('[native-mail]', error);
    window.dispatchEvent(new CustomEvent('erp-native-push-error', { detail: error.message }));
  }

  function cancelled() {
    const error = new Error('Inscription aux notifications annulée après un changement de session.');
    error.code = 'NATIVE_PUSH_CANCELLED';
    return error;
  }

  function cancelRegistration() {
    const reject = rejectRegistration;
    resolveRegistration = null;
    rejectRegistration = null;
    reject?.(cancelled());
  }

  function listen() {
    if (listening) return listening;
    listening = (async () => {
      if (!push || !local) throw new Error('Mettez à jour votre APK pour activer les notifications Android.');
      await push.addListener('registration', async ({ value }) => {
        const auth = registrationAuth;
        const revision = registrationRevision;
        const resolve = resolveRegistration;
        const reject = rejectRegistration;
        try {
          if (!auth || auth !== localStorage.getItem('erp_token')) return;
          await api('/api/push/native/subscribe', { token: value, appId }, auth);
          if (auth !== registrationAuth || auth !== localStorage.getItem('erp_token')) {
            await api('/api/push/native/unsubscribe', { token: value, appId }, auth);
            throw cancelled();
          }
          if (revision !== registrationRevision) throw cancelled();
          localStorage.setItem(tokenKey, value);
          boundAuth = auth;
          resolve?.(true);
        } catch (error) {
          if (reject) {
            if (error.code !== 'NATIVE_PUSH_CANCELLED') console.error('[native-mail] Inscription impossible', error);
            reject(error);
          }
          else if (error.code !== 'NATIVE_PUSH_CANCELLED') report(error);
        }
      });
      await push.addListener('registrationError', error => {
        if (!registrationAuth) return;
        const failure = new Error(error.error || 'Inscription Firebase impossible. Vérifiez la configuration de l’APK.');
        if (rejectRegistration) rejectRegistration(failure);
        else report(failure);
      });
      await push.addListener('pushNotificationActionPerformed', event => {
        try { window.ERPMobile.openNotification(event.notification?.data?.url); }
        catch (error) { report(error); }
      });
      await local.addListener('localNotificationActionPerformed', event => {
        try { window.ERPMobile.openNotification(event.notification?.extra?.url); }
        catch (error) { report(error); }
      });
      await push.addListener('pushNotificationReceived', async notification => {
        try {
          if (!boundAuth || boundAuth !== localStorage.getItem('erp_token')) return;
          window.dispatchEvent(new Event('erp-mail-received'));
          if ((await push.checkPermissions()).receive !== 'granted') return;
          await local.schedule({ notifications: [{
            id: Math.floor(Date.now() % 2147483647),
            title: notification.title || 'Nouveau message',
            body: notification.body || '',
            channelId, smallIcon: 'ic_stat_mail',
            extra: { url: notification.data?.url || '/messaging.html' },
          }] });
        } catch (error) { report(error); }
      });
    })();
    return listening;
  }

  async function checkPermission() {
    await listen();
    return (await push.checkPermissions()).receive;
  }

  async function enable({ requestPermission = false } = {}) {
    const revision = ++registrationRevision;
    cancelRegistration();
    const auth = localStorage.getItem('erp_token');
    if (!auth) throw new Error('Connectez-vous avant l’inscription aux notifications.');
    await listen();
    let permission = await push.checkPermissions();
    if (permission.receive !== 'granted' && requestPermission) permission = await push.requestPermissions();
    if (permission.receive !== 'granted') {
      throw new Error('Autorisez les notifications dans les réglages Android pour recevoir le son et les alertes.');
    }
    const config = await api('/api/push/native/status', undefined, auth);
    if (!config.enabled) throw new Error('Firebase Cloud Messaging n’est pas encore configuré sur le serveur.');
    await push.createChannel({
      id: channelId, name: 'Messages internes',
      description: 'Nouveaux courriels RyanERP et Portail Safari',
      importance: 5, visibility: 1, vibration: true,
    });
    if (revision !== registrationRevision || auth !== localStorage.getItem('erp_token')) throw cancelled();
    registrationAuth = auth;
    let timeout;
    const registered = new Promise((resolve, reject) => {
      resolveRegistration = resolve;
      rejectRegistration = reject;
      timeout = setTimeout(() => reject(new Error('Firebase ne répond pas. Vérifiez Internet et les services Google Play.')), 20000);
    });
    try {
      await Promise.all([registered, push.register()]);
      return true;
    } finally {
      clearTimeout(timeout);
      if (revision === registrationRevision) {
        resolveRegistration = null;
        rejectRegistration = null;
      }
    }
  }

  async function test() {
    const token = localStorage.getItem(tokenKey);
    if (!token) throw new Error('Les notifications ne sont pas encore prêtes sur cet appareil.');
    await api('/api/push/native/test', { token, appId });
  }

  async function unsubscribe() {
    registrationRevision += 1;
    registrationAuth = '';
    boundAuth = '';
    cancelRegistration();
    const token = localStorage.getItem(tokenKey);
    if (token) await api('/api/push/native/unsubscribe', { token, appId });
    if (push) await push.unregister();
    if (local) await local.removeAllDeliveredNotifications();
    localStorage.removeItem(tokenKey);
  }

  window.ERPNativePush = { checkPermission, enable, test, unsubscribe };
  listen().catch(report);
})();
