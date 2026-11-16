const fs = require('node:fs');

const ANDROID_APPS = new Set(['com.ryan.erp', 'com.ryan.portailsafari']);
const MAIL_CHANNEL = 'internal_mail';

function validNativeSubscription(token, appId) {
  return typeof token === 'string' && /^[A-Za-z0-9:_-]{32,4096}$/.test(token) && ANDROID_APPS.has(appId);
}

function createNativePush({ all, run, messaging, env = process.env, logger = console }) {
  let sender = messaging;
  if (!sender && (env.FIREBASE_SERVICE_ACCOUNT_JSON || env.FIREBASE_SERVICE_ACCOUNT_FILE)) {
    const { initializeApp, cert } = require('firebase-admin/app');
    const { getMessaging } = require('firebase-admin/messaging');
    const account = JSON.parse(env.FIREBASE_SERVICE_ACCOUNT_JSON
      || fs.readFileSync(env.FIREBASE_SERVICE_ACCOUNT_FILE, 'utf8'));
    sender = getMessaging(initializeApp({ credential: cert(account) }, 'internal-mail'));
  }

  async function send(userIds, payload, deviceToken) {
    const ids = [...new Set(userIds.map(Number).filter(id => Number.isInteger(id) && id > 0))];
    if (!ids.length) return 0;
    const subscriptions = await all(`SELECT s.token, s.appId FROM native_push_subscriptions s
      JOIN users u ON u.id = s.userId WHERE u.deactivatedAt = ''
      AND s.userId IN (${ids.map(() => '?').join(',')})${deviceToken ? ' AND s.token = ?' : ''}`,
    deviceToken ? [...ids, deviceToken] : ids);
    if (!subscriptions.length) return 0;
    if (!sender) throw new Error('Firebase Cloud Messaging non configuré sur le serveur.');
    let delivered = 0;
    let failure;
    // Keep batches below FCM's 500-token multicast limit.
    for (let offset = 0; offset < subscriptions.length; offset += 500) {
      const batch = subscriptions.slice(offset, offset + 500);
      let result;
      try {
        result = await sender.sendEachForMulticast({
        tokens: batch.map(subscription => subscription.token),
        notification: { title: String(payload.title || 'Nouveau message'), body: String(payload.message || '') },
        data: { url: String(payload.url || '/messaging.html'), tag: String(payload.tag || 'internal-mail') },
        android: {
          priority: 'high', ttl: 86400000,
          notification: {
            channelId: MAIL_CHANNEL, sound: 'default', icon: 'ic_stat_mail',
            tag: String(payload.tag || 'internal-mail'), visibility: 'public',
          },
        },
        });
      } catch (error) {
        logger.error('[native-push] Lot non livré', { code: error?.code, count: batch.length });
        failure = error;
        continue;
      }
      for (let index = 0; index < result.responses.length; index += 1) {
        const response = result.responses[index];
        if (response.success) {
          delivered += 1;
        } else if (['messaging/registration-token-not-registered', 'messaging/invalid-registration-token'].includes(response.error?.code)) {
          await run('DELETE FROM native_push_subscriptions WHERE token = ?', [batch[index].token]);
        } else {
          logger.error('[native-push] Livraison impossible', { appId: batch[index].appId, code: response.error?.code });
          failure = response.error || new Error('Échec de livraison Firebase.');
        }
      }
    }
    if (failure) throw failure;
    return delivered;
  }

  return { enabled: Boolean(sender), send };
}

module.exports = { createNativePush, validNativeSubscription, MAIL_CHANNEL };
