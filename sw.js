self.addEventListener('push', event => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (_) {}
  const title = data.title || 'FloodGuard BD';
  const options = { body: data.body || 'New flood-risk update.', icon: '/favicon.ico', badge: '/favicon.ico', tag: 'floodguard-alert' };
  event.waitUntil(self.registration.showNotification(title, options));
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(list => {
    if (list.length) return list[0].focus();
    return clients.openWindow('/');
  }));
});
