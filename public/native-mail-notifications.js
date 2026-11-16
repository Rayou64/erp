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

  async function api(path, body) {
    const response = await fetch(path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: 'Bearer ' + localStorage.getItem('erp_token'), 'Content-Type': 'application/json' },
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

  function listen() {
    if (listening) return listening;
    listening = (async () => {
      if (!push || !local) throw new Error('Mettez à jour votre APK pour activer les notifications Android.');
      await push.addListener('registration', async ({ value }) => {
        try {
          if (!registrationAuth || registrationAuth !== localStorage.getItem('erp_token')) return;
          await api('/api/push/native/subscribe', { token: value, appId });
          localStorage.setItem(tokenKey, value);
          resolveRegistration?.(true);
        } catch (error) {
          if (rejectRegistration) rejectRegistration(error);
          else report(error);
        }
      });
      await push.addListener('registrationError', error => {
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
          if (!localStorage.getItem('erp_token')) return;
          await local.schedule({ notifications: [{
            id: Math.floor(Date.now() % 2147483647),
            title: notification.title || 'Nouveau message',
            body: notification.body || '',
            channelId, smallIcon: 'ic_stat_mail',
            extra: { url: notification.data?.url || '/messaging.html' },
          }] });
          window.dispatchEvent(new Event('erp-mail-received'));
        } catch (error) { report(error); }
      });
    })();
    return listening;
  }

  async function enable({ requestPermission = false } = {}) {
    await listen();
    let permission = await push.checkPermissions();
    if (permission.receive !== 'granted' && requestPermission) permission = await push.requestPermissions();
    if (permission.receive !== 'granted') {
      throw new Error('Autorisez les notifications dans les réglages Android pour recevoir le son et les alertes.');
    }
    const config = await api('/api/push/native/status');
    if (!config.enabled) throw new Error('Firebase Cloud Messaging n’est pas encore configuré sur le serveur.');
    await push.createChannel({
      id: channelId, name: 'Messages internes',
      description: 'Nouveaux courriels RyanERP et Portail Safari',
      importance: 5, visibility: 1, vibration: true,
    });
    registrationAuth = localStorage.getItem('erp_token');
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
      resolveRegistration = null;
      rejectRegistration = null;
    }
  }

  async function test() {
    const token = localStorage.getItem(tokenKey);
    if (!token) throw new Error('Activez d’abord les notifications sur cet appareil.');
    await api('/api/push/native/test', { token, appId });
  }

  async function unsubscribe() {
    registrationAuth = '';
    const token = localStorage.getItem(tokenKey);
    if (token) await api('/api/push/native/unsubscribe', { token, appId });
    if (push) await push.unregister();
    if (local) await local.removeAllDeliveredNotifications();
    localStorage.removeItem(tokenKey);
  }

  window.ERPNativePush = { enable, test, unsubscribe };
  listen().catch(report);
})();
