self.addEventListener('install', () => {
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', event => {
  let data = {};
  try {
    data = event.data ? event.data.json() : {};
  } catch (_) {
    data = { message: event.data ? String(event.data.text() || '') : '' };
  }

  const title = String(data.title || 'Notification ERP');
  const body = String(data.message || 'Nouvelle activite detectee.');
  const moduleName = String(data.module || 'dashboard').trim() || 'dashboard';
  const targetUrl = String(data.url || `/erp.html?openModule=${encodeURIComponent(moduleName)}`);

  event.waitUntil(
    self.registration.showNotification(title, {
      body,
      data: {
        url: targetUrl,
        module: moduleName,
      },
      tag: String(data.tag || `erp-${moduleName}`),
      renotify: false,
      requireInteraction: false,
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = String(event.notification?.data?.url || '/erp.html');

  event.waitUntil((async () => {
    const allClients = await self.clients.matchAll({
      type: 'window',
      includeUncontrolled: true,
    });

    for (const client of allClients) {
      try {
        const currentUrl = new URL(client.url);
        if (currentUrl.pathname.endsWith('/erp.html') || currentUrl.pathname === '/' || currentUrl.pathname === '') {
          await client.focus();
          if (typeof client.navigate === 'function') {
            await client.navigate(url);
          }
          return;
        }
      } catch (_) {
        // Ignore malformed URL clients.
      }
    }

    await self.clients.openWindow(url);
  })());
});
