(function () {
  'use strict';

  let pending = null;
  let suspended = false;
  let lastNotice = '';
  let currentStatus = { message: '', enabled: false };
  const isIos = /iPad|iPhone|iPod/.test(navigator.userAgent)
    || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  const standalone = window.matchMedia('(display-mode: standalone)').matches || navigator.standalone === true;

  function unavailableReason() {
    if (!window.isSecureContext) return 'Les alertes sur téléphone nécessitent une adresse HTTPS.';
    if (window.ERPNativePush) return '';
    if (window.Capacitor?.isNativePlatform?.() && !window.ERPNativePush) return 'Mettez à jour votre APK pour activer les notifications Android.';
    if (isIos && !standalone) return 'Sur iPhone : Safari > Partager > Sur l’écran d’accueil. Ouvrez ensuite cette icône pour activer les alertes (iOS 16.4 ou plus récent).';
    if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) {
      return 'Ce navigateur ne prend pas en charge les alertes. Utilisez Safari sur iPhone ou Chrome sur Android à jour.';
    }
    return '';
  }

  function status(message, enabled = false) {
    currentStatus = { message, enabled };
    if (enabled) lastNotice = '';
    window.dispatchEvent(new CustomEvent('erp-push-status', { detail: { ...currentStatus } }));
  }

  function feedback(message, type = 'error') {
    if (lastNotice === message) return;
    lastNotice = message;
    const show = () => {
      if (typeof window.showMessage === 'function') window.showMessage(message, type);
      else if (typeof window.toast === 'function') window.toast(message);
      else window.alert(message);
    };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', show, { once: true });
    else show();
  }

  function failure(error, source) {
    if (error.code === 'NATIVE_PUSH_CANCELLED') {
      status('');
      return false;
    }
    console.error(source, error);
    status(error.message);
    if (!suspended) feedback('Notifications : ' + error.message);
    return false;
  }

  function isCurrent(auth) {
    return !suspended && auth === localStorage.getItem('erp_token');
  }

  function complete(auth) {
    pending = null;
    const currentAuth = localStorage.getItem('erp_token');
    if (!suspended && currentAuth && currentAuth !== auth) enable();
  }

  async function api(url, body, auth = localStorage.getItem('erp_token')) {
    const response = await fetch(url, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: 'Bearer ' + auth, 'Content-Type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Le service de notifications est indisponible.');
    return data;
  }

  function applicationKey(value) {
    const padded = value + '='.repeat((4 - value.length % 4) % 4);
    return Uint8Array.from(atob(padded.replace(/-/g, '+').replace(/_/g, '/')), character => character.charCodeAt(0));
  }

  function enable(options = {}) {
    if (options.requestPermission) suspended = false;
    const auth = localStorage.getItem('erp_token');
    if (suspended || !auth) {
      status('');
      return Promise.resolve(false);
    }
    if (pending) return pending;
    const reason = unavailableReason();
    if (reason) {
      status(reason);
      if (options.requestPermission) feedback(reason, 'warning');
      return Promise.resolve(false);
    }
    if (window.ERPNativePush) {
      pending = (async () => {
        const permission = await window.ERPNativePush.checkPermission();
        if (permission !== 'granted' && !options.requestPermission) {
          status('Les notifications restent soumises à votre autorisation Android.');
          if (permission === 'denied') feedback('Autorisez les notifications dans les réglages Android pour recevoir les nouveaux messages.', 'warning');
          return false;
        }
        if (!isCurrent(auth)) return false;
        await window.ERPNativePush.enable(options);
        if (!isCurrent(auth)) return false;
        status('Notifications du téléphone activées : son et vibration selon vos réglages Android.', true);
        return true;
      })().catch(error => failure(error, '[native-push]')).finally(() => complete(auth));
      return pending;
    }
    if (Notification.permission === 'denied') {
      const message = 'Notifications bloquées : autorisez-les dans les réglages du navigateur ou du téléphone, puis réessayez.';
      status(message);
      if (options.requestPermission) feedback(message, 'warning');
      return Promise.resolve(false);
    }
    // Safari requires the permission request directly inside the user's click.
    const permission = Notification.permission === 'default' && options.requestPermission
      ? Notification.requestPermission()
      : Promise.resolve(Notification.permission);
    pending = (async () => {
      try {
        if (await permission !== 'granted') {
          status('Les notifications restent soumises à votre autorisation du navigateur.');
          return false;
        }
        if (!isCurrent(auth)) return false;
        const key = await api('/api/push/public-key', undefined, auth);
        if (!key.enabled || !key.publicKey) throw new Error('Les notifications ne sont pas configurées sur le serveur.');
        await navigator.serviceWorker.register('/sw.js', { scope: '/' });
        const registration = await navigator.serviceWorker.ready;
        let subscription = await registration.pushManager.getSubscription();
        const desiredKey = applicationKey(key.publicKey);
        const existingKey = subscription?.options?.applicationServerKey;
        if (subscription && existingKey && String(new Uint8Array(existingKey)) !== String(desiredKey)) {
          await subscription.unsubscribe();
          subscription = null;
        }
        if (!subscription) {
          subscription = await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: desiredKey });
        }
        if (!isCurrent(auth)) return false;
        await api('/api/push/subscribe', { subscription }, auth);
        if (!isCurrent(auth)) {
          const currentAuth = localStorage.getItem('erp_token');
          if (suspended && (!currentAuth || currentAuth === auth)) await subscription.unsubscribe();
          await api('/api/push/unsubscribe', { endpoint: subscription.endpoint }, auth);
          return false;
        }
        status('Alertes courriel activées sur cet appareil.', true);
        return true;
      } catch (error) {
        return failure(error, '[push]');
      } finally {
        complete(auth);
      }
    })();
    return pending;
  }

  async function test() {
    try {
      if (window.ERPNativePush) await window.ERPNativePush.test();
      else await api('/api/push/test', {});
      status('Test envoyé. Vérifiez la notification sur votre téléphone.', true);
      feedback('Test envoyé. Vérifiez la notification sur votre téléphone.', 'success');
      return true;
    } catch (error) {
      return failure(error, '[push-test]');
    }
  }

  async function unsubscribe() {
    suspended = true;
    status('');
    const auth = localStorage.getItem('erp_token');
    if (window.ERPNativePush) return window.ERPNativePush.unsubscribe();
    if (!('serviceWorker' in navigator)) return;
    const registration = await navigator.serviceWorker.getRegistration('/');
    const subscription = await registration?.pushManager?.getSubscription();
    if (subscription) {
      await api('/api/push/unsubscribe', { endpoint: subscription.endpoint }, auth);
      await subscription.unsubscribe();
    }
  }

  function init() {
    enable();
    window.addEventListener('focus', () => enable());
    window.addEventListener('online', () => enable());
    window.addEventListener('erp-native-notification-permission', () => enable());
    window.addEventListener('storage', event => { if (event.key === 'erp_token') enable(); });
    document.addEventListener('visibilitychange', () => { if (!document.hidden) enable(); });
    window.addEventListener('erp-native-push-error', event => failure(new Error(String(event.detail)), '[native-push]'));
    navigator.serviceWorker?.addEventListener('message', event => {
      if (event.data?.type === 'push-received') window.dispatchEvent(new Event('erp-mail-received'));
    });
  }

  window.ERPPush = { enable, test, unsubscribe, getStatus: () => ({ ...currentStatus }) };
  init();
})();
