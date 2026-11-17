(function () {
  'use strict';
  const messagingOnly = window.RyanNative?.isMessagingOnly?.() === true;
  const native = messagingOnly || window.Capacitor?.isNativePlatform?.() === true
    || typeof window.RyanNative?.getStatus === 'function';
  if (messagingOnly) document.documentElement.classList.add('messaging-only-app');
  const pendingKey = 'erp_pending_mail_url';
  const entryConversation = new URLSearchParams(location.search).get('conversation');
  if (native && location.pathname === '/index.html' && entryConversation !== null) {
    sessionStorage.setItem(pendingKey, mailUrl('/messaging.html?conversation=' + entryConversation));
  }
  if (messagingOnly || new URLSearchParams(location.search).get('mail') === '1') {
    sessionStorage.setItem('erp_mail_entry', '1');
  }

  function mailUrl(value) {
    const url = new URL(String(value || '/messaging.html'), location.origin);
    if (url.origin !== location.origin || url.pathname !== '/messaging.html') {
      throw new Error('Lien de notification de messagerie invalide.');
    }
    const id = url.searchParams.get('conversation');
    if (id !== null && !/^[1-9]\d*$/.test(id)) throw new Error('Conversation de notification invalide.');
    return '/messaging.html' + (id ? '?conversation=' + id : '');
  }

  function destination() {
    const pending = sessionStorage.getItem(pendingKey);
    if (pending) return mailUrl(pending);
    return messagingOnly || sessionStorage.getItem('erp_mail_entry') === '1' ? '/messaging.html' : '/erp.html';
  }

  function loginUrl() {
    return '/index.html?mail=1';
  }

  function openNotification(value) {
    const url = mailUrl(value);
    sessionStorage.setItem(pendingKey, url);
    if (location.pathname === '/index.html') return;
    location.assign(localStorage.getItem('erp_token') ? url : loginUrl());
  }

  window.ERPMobile = {
    native, messagingOnly, appId: messagingOnly ? 'com.ryan.portailsafari' : 'com.ryan.erp',
    destination, loginUrl, openNotification,
    clearPending: () => sessionStorage.removeItem(pendingKey),
  };
})();
