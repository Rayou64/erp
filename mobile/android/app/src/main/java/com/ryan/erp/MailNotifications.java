package com.ryan.erp;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Bitmap;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.net.Uri;
import android.os.Build;
import android.service.notification.StatusBarNotification;
import android.util.Log;
import androidx.core.app.NotificationCompat;
import androidx.core.app.NotificationManagerCompat;
import androidx.core.app.Person;
import androidx.core.content.ContextCompat;
import androidx.core.graphics.drawable.IconCompat;
import java.util.Locale;
import java.util.Map;

final class MailNotifications {
    static final String PREFS = "native_mail";
    static final String CHANNEL = "internal_mail";
    static final String SAFARI_PACKAGE = "com.ryan.portailsafari";
    static final String EXTRA_URL = "mail_url";
    private static final String TAG_PREFIX = "mail:";
    private static final int ID = 8100;

    static boolean safariInstalled(Context context) {
        try {
            return context.getPackageManager().getApplicationInfo(SAFARI_PACKAGE, 0).enabled;
        } catch (PackageManager.NameNotFoundException absent) {
            return false;
        }
    }

    static String mailPath(String value) {
        if (value == null) return null;
        Uri uri = Uri.parse(value);
        if (!"/messaging.html".equals(uri.getPath()) || uri.getFragment() != null) return null;
        if (uri.isAbsolute() && (!"https".equals(uri.getScheme())
            || !"ryanerp-hn5zd.ondigitalocean.app".equals(uri.getHost())
            || uri.getUserInfo() != null || (uri.getPort() != -1 && uri.getPort() != 443))) return null;
        if (!uri.isAbsolute() && uri.getAuthority() != null) return null;
        String conversation = uri.getQueryParameter("conversation");
        if (conversation != null && !conversation.matches("[1-9][0-9]*")) return null;
        return "/messaging.html" + (conversation == null ? "" : "?conversation=" + conversation);
    }

    static void setAccount(Context context, int userId) {
        int previous = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getInt("userId", 0);
        if (previous != userId || userId == 0) clear(context);
        context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().putInt("userId", userId).apply();
    }

    private static void clear(Context context) {
        NotificationManager manager = context.getSystemService(NotificationManager.class);
        for (StatusBarNotification notification : manager.getActiveNotifications()) {
            if (notification.getTag() != null && notification.getTag().startsWith(TAG_PREFIX)) {
                manager.cancel(notification.getTag(), notification.getId());
            }
        }
    }

    private static Bitmap avatar(String name) {
        Bitmap bitmap = Bitmap.createBitmap(96, 96, Bitmap.Config.ARGB_8888);
        Canvas canvas = new Canvas(bitmap);
        Paint paint = new Paint(Paint.ANTI_ALIAS_FLAG);
        paint.setColor(Color.rgb(37, 99, 235));
        canvas.drawCircle(48, 48, 48, paint);
        StringBuilder initials = new StringBuilder();
        for (String part : name.trim().split("\\s+")) {
            if (!part.isEmpty() && initials.length() < 2) initials.appendCodePoint(part.codePointAt(0));
        }
        paint.setColor(Color.WHITE);
        paint.setTextSize(36);
        paint.setTypeface(android.graphics.Typeface.DEFAULT_BOLD);
        paint.setTextAlign(Paint.Align.CENTER);
        canvas.drawText(initials.toString().toUpperCase(Locale.ROOT), 48, 48 - (paint.ascent() + paint.descent()) / 2, paint);
        return bitmap;
    }

    static synchronized void show(Context context, Map<String, String> data, long sentAt) {
        String currentUser = String.valueOf(context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).getInt("userId", 0));
        if ("0".equals(currentUser) || !currentUser.equals(data.get("userId"))) return;
        if (!BuildConfig.MESSAGING_ONLY && safariInstalled(context)) {
            clear(context);
            return;
        }
        if (Build.VERSION.SDK_INT >= 33
            && ContextCompat.checkSelfPermission(context, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) return;
        if (!NotificationManagerCompat.from(context).areNotificationsEnabled()) return;
        String path = mailPath(data.get("url"));
        if (path == null) {
            Log.e("native-mail", "Invalid notification conversation URL");
            return;
        }
        NotificationManager manager = context.getSystemService(NotificationManager.class);
        if (Build.VERSION.SDK_INT >= 26) {
            NotificationChannel channel = new NotificationChannel(CHANNEL, "Messages internes", NotificationManager.IMPORTANCE_HIGH);
            channel.setDescription("Nouveaux messages RyanERP et Portail Safari");
            channel.enableVibration(true);
            manager.createNotificationChannel(channel);
        }
        String tag = TAG_PREFIX + path;
        NotificationCompat.MessagingStyle style = null;
        for (StatusBarNotification active : manager.getActiveNotifications()) {
            if (tag.equals(active.getTag())) style = NotificationCompat.MessagingStyle.extractMessagingStyleFromNotification(active.getNotification());
        }
        if (style == null) style = new NotificationCompat.MessagingStyle(new Person.Builder().setName("Vous").build());
        String messageTag = data.get("tag");
        for (NotificationCompat.MessagingStyle.Message message : style.getMessages()) {
            if (messageTag != null && messageTag.equals(message.getExtras().getString("mailTag"))) return;
        }
        String sender = data.getOrDefault("senderName", "Nouveau message");
        String subject = data.getOrDefault("subject", "Messagerie interne");
        String preview = data.getOrDefault("preview", "");
        Bitmap avatar = avatar(sender);
        Person person = new Person.Builder().setName(sender).setIcon(IconCompat.createWithBitmap(avatar)).build();
        NotificationCompat.MessagingStyle.Message message = new NotificationCompat.MessagingStyle.Message(
            preview, sentAt > 0 ? sentAt : System.currentTimeMillis(), person
        );
        message.getExtras().putString("mailTag", messageTag);
        style.setConversationTitle(subject).setGroupConversation(true).addMessage(message);
        Intent intent = new Intent(context, MainActivity.class)
            .setAction(Intent.ACTION_VIEW).setData(Uri.parse("ryanmail:" + path))
            .addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP)
            .putExtra(EXTRA_URL, path);
        PendingIntent open = PendingIntent.getActivity(context, 0, intent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        Notification notification = new NotificationCompat.Builder(context, CHANNEL)
            .setSmallIcon(R.drawable.ic_stat_mail).setLargeIcon(avatar)
            .setColor(Color.rgb(37, 99, 235)).setContentTitle(sender).setContentText(preview)
            .setSubText(subject).setStyle(style).setContentIntent(open).setAutoCancel(true)
            .setCategory(NotificationCompat.CATEGORY_MESSAGE).setPriority(NotificationCompat.PRIORITY_HIGH)
            .setDefaults(NotificationCompat.DEFAULT_ALL).setVisibility(NotificationCompat.VISIBILITY_PUBLIC)
            .build();
        manager.notify(tag, ID, notification);
    }
}
