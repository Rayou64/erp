const STATIC_CACHE = 'ryanerp-static-v1';
const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/erp.html',
  '/manifest.webmanifest',
  '/assets/icons/icon-192.png',
  '/assets/icons/icon-512.png',
  '/assets/icons/apple-touch-icon-180.png',
  '/assets/icons/favicon-32.png',
];

self.addEventListener('install', event => {
  event.waitUntil((async () => {
    const cache = await caches.open(STATIC_CACHE);
    await cache.addAll(STATIC_ASSETS);
    await self.skipWaiting();
  })());
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys
      .filter(key => key !== STATIC_CACHE)
      .map(key => caches.delete(key)));
    await self.clients.claim();
  })());
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') return;

  const requestUrl = new URL(request.url);
  if (requestUrl.origin !== self.location.origin) return;
  if (requestUrl.pathname.startsWith('/api/')) return;

  if (request.mode === 'navigate') {
    event.respondWith((async () => {
      try {
        const networkResponse = await fetch(request);
        const cache = await caches.open(STATIC_CACHE);
        cache.put('/index.html', networkResponse.clone());
        return networkResponse;
      } catch (_) {
        const cached = await caches.match('/index.html');
        if (cached) return cached;
        return Response.error();
      }
    })());
    return;
  }

  const staticAssetRegex = /\.(?:css|js|png|jpg|jpeg|svg|webp|ico|woff2?|ttf|json|webmanifest)$/i;
  const isStaticAsset = staticAssetRegex.test(requestUrl.pathname);
  if (!isStaticAsset) return;

  event.respondWith((async () => {
    const cached = await caches.match(request);
    if (cached) return cached;
    try {
      const networkResponse = await fetch(request);
      const cache = await caches.open(STATIC_CACHE);
      cache.put(request, networkResponse.clone());
      return networkResponse;
    } catch (_) {
      return Response.error();
    }
  })());
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
