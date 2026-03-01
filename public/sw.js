// Голубь PWA Service Worker v6
const CACHE = 'golub-v6';
const STATIC = ['/'];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(STATIC)).then(() => self.skipWaiting()));
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  if (e.request.url.includes('/api/') || e.request.url.includes('ws://') || e.request.url.includes('wss://')) return;
  if (e.request.method !== 'GET') return;
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request))
  );
});

// ─── Push уведомления ─────────────────────────────────────────────────────────
self.addEventListener('push', e => {
  if (!e.data) return;
  let data;
  try { data = e.data.json(); } catch { data = { title: '🕊️ Голубь', body: e.data.text() }; }
  e.waitUntil(
    self.registration.showNotification(data.title || '🕊️ Голубь', {
      body: data.body || 'Новое сообщение',
      icon: '/icons/icon-192.png',
      badge: '/icons/icon-192.png',
      tag: data.chatId || 'golub-msg',
      renotify: true,
      data: { chatId: data.chatId },
      vibrate: [100, 50, 100],
      requireInteraction: false,
    })
  );
});

// ─── Клик по уведомлению ───────────────────────────────────────────────────────
self.addEventListener('notificationclick', e => {
  e.notification.close();
  const chatId = e.notification.data?.chatId;

  e.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clients => {
      // Ищем открытую вкладку приложения
      const appClient = clients.find(c => c.url.startsWith(self.location.origin));
      if (appClient) {
        // Фокусируем и отправляем команду открыть чат
        return appClient.focus().then(fc => {
          if (chatId) fc.postMessage({ type: 'open_chat', chatId });
        });
      }
      // Нет открытой вкладки — открываем новую
      return self.clients.openWindow(self.location.origin + (chatId ? `?chat=${chatId}` : ''));
    })
  );
});

// ─── Сообщения от клиента ─────────────────────────────────────────────────────
self.addEventListener('message', e => {
  if (e.data?.type === 'SHOW_NOTIFICATION') {
    const { title, body, chatId } = e.data;
    self.registration.showNotification(title, {
      body,
      icon: '/icons/icon-192.png',
      badge: '/icons/icon-192.png',
      tag: chatId || 'golub-msg',
      renotify: true,
      data: { chatId },
      vibrate: [100, 50, 100],
    });
  }
  // Клиент сообщает что он активен (для skip waiting)
  if (e.data?.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
