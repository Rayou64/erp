package com.ryan.erp;

import android.app.Activity;
import android.app.AlarmManager;
import android.app.AlertDialog;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageInfo;
import android.net.Uri;
import android.os.Build;
import android.provider.Settings;
import android.util.Log;
import androidx.core.app.NotificationCompat;
import androidx.core.content.FileProvider;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.json.JSONObject;

final class AppUpdateManager {
    static final String ACTION_CHECK = "com.ryan.erp.CHECK_UPDATE";
    static final String ACTION_INSTALL = "com.ryan.erp.INSTALL_UPDATE";

    private static final String TAG = "RyanERP.Update";
    private static final String MANIFEST_URL =
        "https://ryanerp-hn5zd.ondigitalocean.app/mobile-version.json";
    private static final String UPDATE_HOST = "ryanerp-hn5zd.ondigitalocean.app";
    private static final String CHANNEL_ID = "ryan_updates";
    private static final String PREFS = "ryan_updates";
    private static final String KEY_PENDING_APK = "pending_apk";
    private static final int UPDATE_NOTIFICATION_ID = 5201;
    private static final int DOWNLOAD_NOTIFICATION_ID = 5202;
    private static final int UPDATE_ALARM_ID = 5203;
    private static final long CHECK_INTERVAL_MS = 6L * 60L * 60L * 1000L;
    private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    private AppUpdateManager() {
    }

    static void schedule(Context context) {
        AlarmManager alarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        if (alarmManager == null) return;
        Intent intent = new Intent(context, UpdateCheckReceiver.class).setAction(ACTION_CHECK);
        PendingIntent pendingIntent = PendingIntent.getBroadcast(
            context,
            UPDATE_ALARM_ID,
            intent,
            PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT
        );
        alarmManager.setInexactRepeating(
            AlarmManager.RTC_WAKEUP,
            System.currentTimeMillis() + 15L * 60L * 1000L,
            CHECK_INTERVAL_MS,
            pendingIntent
        );
    }

    static void checkForUpdate(Context context, Runnable completion) {
        Context appContext = context.getApplicationContext();
        EXECUTOR.execute(() -> {
            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) new URL(MANIFEST_URL).openConnection();
                connection.setConnectTimeout(15000);
                connection.setReadTimeout(20000);
                connection.setUseCaches(false);
                connection.setRequestProperty("Accept", "application/json");
                int status = connection.getResponseCode();
                if (status != HttpURLConnection.HTTP_OK) {
                    throw new IllegalStateException("HTTP " + status);
                }
                JSONObject manifest = new JSONObject(readText(connection.getInputStream()));
                int versionCode = manifest.getInt("versionCode");
                if (versionCode > currentVersionCode(appContext)) {
                    notifyUpdate(
                        appContext,
                        versionCode,
                        manifest.getString("versionName"),
                        manifest.getString("apkUrl"),
                        manifest.getString("sha256"),
                        manifest.optString("releaseNotes", "")
                    );
                }
            } catch (Exception error) {
                Log.w(TAG, "Echec de la verification de mise a jour", error);
            } finally {
                if (connection != null) connection.disconnect();
                if (completion != null) completion.run();
            }
        });
    }

    private static String readText(InputStream input) throws Exception {
        try (InputStream stream = input; ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int count;
            while ((count = stream.read(buffer)) != -1) {
                output.write(buffer, 0, count);
                if (output.size() > 65536) throw new IllegalStateException("Manifeste trop volumineux");
            }
            return output.toString("UTF-8");
        }
    }

    private static void createChannel(Context context) {
        if (Build.VERSION.SDK_INT < 26) return;
        NotificationManager manager =
            (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager == null) return;
        NotificationChannel channel = new NotificationChannel(
            CHANNEL_ID,
            "Mises a jour RyanERP",
            NotificationManager.IMPORTANCE_DEFAULT
        );
        channel.setDescription("Nouvelles versions de l'application Ryan ERP Mobile");
        manager.createNotificationChannel(channel);
    }

    private static void notifyUpdate(
        Context context,
        int versionCode,
        String versionName,
        String apkUrl,
        String sha256,
        String releaseNotes
    ) throws Exception {
        validateUpdateUrl(apkUrl);
        createChannel(context);
        Intent intent = new Intent(context, MainActivity.class)
            .setAction(ACTION_INSTALL)
            .putExtra("versionCode", versionCode)
            .putExtra("versionName", versionName)
            .putExtra("apkUrl", apkUrl)
            .putExtra("sha256", sha256)
            .putExtra("releaseNotes", releaseNotes);
        PendingIntent pendingIntent = PendingIntent.getActivity(
            context,
            versionCode,
            intent,
            PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT
        );
        NotificationCompat.Builder notification = new NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(context.getApplicationInfo().icon)
            .setContentTitle("Mise a jour RyanERP disponible")
            .setContentText("Version " + versionName + " - Touchez pour mettre a jour")
            .setStyle(new NotificationCompat.BigTextStyle().bigText(
                releaseNotes.isEmpty()
                    ? "Une nouvelle version de Ryan ERP Mobile est disponible."
                    : releaseNotes
            ))
            .setAutoCancel(true)
            .setContentIntent(pendingIntent)
            .addAction(0, "Mettre a jour", pendingIntent)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT);
        NotificationManager manager =
            (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager != null) manager.notify(UPDATE_NOTIFICATION_ID, notification.build());
    }

    static void promptFromIntent(Activity activity, Intent intent) {
        if (intent == null || !ACTION_INSTALL.equals(intent.getAction())) return;
        intent.setAction(null);
        String versionName = intent.getStringExtra("versionName");
        String apkUrl = intent.getStringExtra("apkUrl");
        String sha256 = intent.getStringExtra("sha256");
        String releaseNotes = intent.getStringExtra("releaseNotes");
        int versionCode = intent.getIntExtra("versionCode", 0);
        if (versionCode <= currentVersionCode(activity) || apkUrl == null || sha256 == null) return;

        activity.getWindow().getDecorView().post(() -> new AlertDialog.Builder(activity)
            .setTitle("Mettre a jour RyanERP ?")
            .setMessage(
                "Version " + versionName + " disponible."
                    + (releaseNotes == null || releaseNotes.isEmpty() ? "" : "\n\n" + releaseNotes)
            )
            .setNegativeButton("Plus tard", null)
            .setPositiveButton("Mettre a jour", (dialog, which) ->
                downloadAndInstall(activity, versionCode, versionName, apkUrl, sha256)
            )
            .show()
        );
    }

    private static void downloadAndInstall(
        Activity activity,
        int versionCode,
        String versionName,
        String apkUrl,
        String expectedSha256
    ) {
        Context appContext = activity.getApplicationContext();
        createChannel(appContext);
        EXECUTOR.execute(() -> {
            HttpURLConnection connection = null;
            File apk = new File(new File(appContext.getCacheDir(), "updates"), "ryanerp-" + versionCode + ".apk");
            try {
                validateUpdateUrl(apkUrl);
                File parent = apk.getParentFile();
                if (parent == null || (!parent.exists() && !parent.mkdirs())) {
                    throw new IllegalStateException("Impossible de preparer le dossier de mise a jour");
                }
                connection = (HttpURLConnection) new URL(apkUrl).openConnection();
                connection.setConnectTimeout(15000);
                connection.setReadTimeout(60000);
                connection.setUseCaches(false);
                int status = connection.getResponseCode();
                if (status != HttpURLConnection.HTTP_OK) {
                    throw new IllegalStateException("Telechargement HTTP " + status);
                }
                long total = connection.getContentLengthLong();
                int lastProgress = -1;
                try (
                    InputStream input = new BufferedInputStream(connection.getInputStream());
                    FileOutputStream output = new FileOutputStream(apk)
                ) {
                    byte[] buffer = new byte[32768];
                    long downloaded = 0;
                    int count;
                    while ((count = input.read(buffer)) != -1) {
                        output.write(buffer, 0, count);
                        downloaded += count;
                        int progress = total > 0 ? (int) Math.min(100, downloaded * 100 / total) : 0;
                        if (progress / 5 != lastProgress / 5) {
                            lastProgress = progress;
                            showDownloadProgress(appContext, versionName, progress, total <= 0);
                        }
                    }
                }
                String actualSha256 = sha256(apk);
                if (!actualSha256.equalsIgnoreCase(expectedSha256)) {
                    throw new SecurityException("La signature SHA-256 de l'APK ne correspond pas");
                }
                PackageInfo info = appContext.getPackageManager().getPackageArchiveInfo(apk.getAbsolutePath(), 0);
                long downloadedVersion = info == null
                    ? 0
                    : (Build.VERSION.SDK_INT >= 28 ? info.getLongVersionCode() : info.versionCode);
                if (
                    info == null
                        || !appContext.getPackageName().equals(info.packageName)
                        || downloadedVersion != versionCode
                        || downloadedVersion <= currentVersionCode(appContext)
                ) {
                    throw new SecurityException("APK RyanERP invalide ou version incorrecte");
                }
                cancelDownloadNotification(appContext);
                activity.runOnUiThread(() -> requestInstall(activity, apk));
            } catch (Exception error) {
                Log.e(TAG, "Echec de la mise a jour", error);
                if (apk.exists() && !apk.delete()) Log.w(TAG, "Impossible de supprimer l'APK invalide");
                showDownloadError(appContext, error.getMessage());
            } finally {
                if (connection != null) connection.disconnect();
            }
        });
    }

    private static void validateUpdateUrl(String apkUrl) throws Exception {
        URI uri = new URI(apkUrl);
        if (!"https".equalsIgnoreCase(uri.getScheme()) || !UPDATE_HOST.equalsIgnoreCase(uri.getHost())) {
            throw new SecurityException("Adresse de mise a jour non autorisee");
        }
    }

    private static long currentVersionCode(Context context) {
        try {
            PackageInfo info = context.getPackageManager().getPackageInfo(context.getPackageName(), 0);
            return Build.VERSION.SDK_INT >= 28 ? info.getLongVersionCode() : info.versionCode;
        } catch (Exception error) {
            Log.e(TAG, "Impossible de lire la version installee", error);
            return Long.MAX_VALUE;
        }
    }

    private static String sha256(File file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = new BufferedInputStream(new java.io.FileInputStream(file))) {
            byte[] buffer = new byte[32768];
            int count;
            while ((count = input.read(buffer)) != -1) digest.update(buffer, 0, count);
        }
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) hex.append(String.format(Locale.US, "%02x", value));
        return hex.toString();
    }

    private static void showDownloadProgress(
        Context context,
        String versionName,
        int progress,
        boolean indeterminate
    ) {
        NotificationCompat.Builder notification = new NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(context.getApplicationInfo().icon)
            .setContentTitle("Mise a jour RyanERP " + versionName)
            .setContentText(indeterminate ? "Telechargement en cours..." : progress + " %")
            .setOnlyAlertOnce(true)
            .setOngoing(true)
            .setProgress(100, progress, indeterminate);
        NotificationManager manager =
            (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager != null) manager.notify(DOWNLOAD_NOTIFICATION_ID, notification.build());
    }

    private static void showDownloadError(Context context, String detail) {
        NotificationCompat.Builder notification = new NotificationCompat.Builder(context, CHANNEL_ID)
            .setSmallIcon(context.getApplicationInfo().icon)
            .setContentTitle("Echec de la mise a jour RyanERP")
            .setContentText(detail == null || detail.isEmpty() ? "Reessayez plus tard." : detail)
            .setAutoCancel(true);
        NotificationManager manager =
            (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager != null) manager.notify(DOWNLOAD_NOTIFICATION_ID, notification.build());
    }

    private static void cancelDownloadNotification(Context context) {
        NotificationManager manager =
            (NotificationManager) context.getSystemService(Context.NOTIFICATION_SERVICE);
        if (manager != null) manager.cancel(DOWNLOAD_NOTIFICATION_ID);
    }

    private static void requestInstall(Activity activity, File apk) {
        if (Build.VERSION.SDK_INT >= 26 && !activity.getPackageManager().canRequestPackageInstalls()) {
            SharedPreferences prefs = activity.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
            prefs.edit().putString(KEY_PENDING_APK, apk.getAbsolutePath()).apply();
            Intent settings = new Intent(
                Settings.ACTION_MANAGE_UNKNOWN_APP_SOURCES,
                Uri.parse("package:" + activity.getPackageName())
            );
            activity.startActivity(settings);
            return;
        }
        openInstaller(activity, apk);
    }

    static void resumePendingInstall(Activity activity) {
        if (Build.VERSION.SDK_INT >= 26 && !activity.getPackageManager().canRequestPackageInstalls()) return;
        SharedPreferences prefs = activity.getSharedPreferences(PREFS, Context.MODE_PRIVATE);
        String path = prefs.getString(KEY_PENDING_APK, "");
        if (path.isEmpty()) return;
        prefs.edit().remove(KEY_PENDING_APK).apply();
        File apk = new File(path);
        if (apk.isFile()) openInstaller(activity, apk);
    }

    private static void openInstaller(Activity activity, File apk) {
        Uri uri = FileProvider.getUriForFile(
            activity,
            activity.getPackageName() + ".fileprovider",
            apk
        );
        Intent install = new Intent(Intent.ACTION_VIEW)
            .setDataAndType(uri, "application/vnd.android.package-archive")
            .addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);
        activity.startActivity(install);
    }
}
