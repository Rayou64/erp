(function () {
  'use strict';

  let pending = null;
  let panel;
  const isIos = /iPad|iPhone|iPod/.test(navigator.userAgent)
    || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  const standalone = window.matchMedia('(display-mode: standalone)').matches || navigator.standalone === true;

  function unavailableReason() {
    if (!window.isSecureContext) return 'Les alertes sur téléphone nécessitent une adresse HTTPS.';
    if (window.Capacitor?.isNativePlatform?.()) return 'Pour les alertes en arrière-plan, ouvrez le site ERP dans Chrome et installez-le sur l’écran d’accueil.';
    if (isIos && !standalone) return 'Sur iPhone : Safari > Partager > Sur l’écran d’accueil. Ouvrez ensuite cette icône pour activer les alertes (iOS 16.4 ou plus récent).';
    if (!('serviceWorker' in navigator) || !('PushManager' in window) || !('Notification' in window)) {
      return 'Ce navigateur ne prend pas en charge les alertes. Utilisez Safari sur iPhone ou Chrome sur Android à jour.';
    }
    return '';
  }

  function status(message, enabled = false) {
    if (!panel) return;
    panel.querySelector('[data-push-status]').textContent = message;
    panel.querySelector('[data-push-enable]').hidden = enabled;
    panel.querySelector('[data-push-test]').hidden = !enabled;
  }

  async function api(url, body) {
    const response = await fetch(url, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: 'Bearer ' + localStorage.getItem('erp_token'), 'Content-Type': 'application/json' },
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
    if (pending) return pending;
    const reason = unavailableReason();
    if (reason) {
      status(reason);
      return Promise.resolve(false);
    }
    if (Notification.permission === 'denied') {
      status('Notifications bloquées : autorisez-les dans les réglages du navigateur ou du téléphone, puis réessayez.');
      return Promise.resolve(false);
    }
    // Safari requires the permission request directly inside the user's click.
    const permission = Notification.permission === 'default' && options.requestPermission
      ? Notification.requestPermission()
      : Promise.resolve(Notification.permission);
    pending = (async () => {
      try {
        if (await permission !== 'granted') {
          status('Activez les alertes pour recevoir vos courriels même lorsque l’ERP est fermé.');
          return false;
        }
        const key = await api('/api/push/public-key');
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
        await api('/api/push/subscribe', { subscription });
        status('Alertes courriel activées sur cet appareil.', true);
        return true;
      } catch (error) {
        console.error('[push]', error);
        status('Activation impossible : ' + error.message);
        return false;
      } finally {
        pending = null;
      }
    })();
    return pending;
  }

  async function test() {
    try {
      await api('/api/push/test', {});
      status('Test envoyé. Vérifiez la notification sur votre téléphone.', true);
    } catch (error) {
      console.error('[push-test]', error);
      status(error.message, true);
    }
  }

  async function unsubscribe() {
    if (!('serviceWorker' in navigator)) return;
    const registration = await navigator.serviceWorker.getRegistration('/');
    const subscription = await registration?.pushManager?.getSubscription();
    if (subscription) {
      await api('/api/push/unsubscribe', { endpoint: subscription.endpoint });
      await subscription.unsubscribe();
    }
  }

  function init() {
    const host = document.querySelector('[data-push-settings-host]');
    if (host) {
      panel = document.createElement('section');
      panel.style.cssText = 'display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:10px 12px;border:1px solid #cbd5e1;border-radius:10px;background:#f8fafc;color:#334155;font:13px system-ui;margin:8px 0';
      panel.setAttribute('aria-label', 'Notifications de courriel');
      panel.innerHTML = '<span data-push-status role="status" style="flex:1;min-width:180px"></span><button type="button" data-push-enable>Activer les alertes courriel</button><button type="button" data-push-test hidden>Tester sur cet appareil</button>';
      host.appendChild(panel);
      panel.querySelector('[data-push-enable]').addEventListener('click', () => enable({ requestPermission: true }));
      panel.querySelector('[data-push-test]').addEventListener('click', test);
    }
    enable();
    navigator.serviceWorker?.addEventListener('message', event => {
      if (event.data?.type === 'push-received') window.dispatchEvent(new Event('erp-mail-received'));
    });
  }

  window.ERPPush = { enable, test, unsubscribe };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
