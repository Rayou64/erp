package com.ryan.erp;

import android.app.AlarmManager;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.ServiceInfo;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.os.Build;
import android.os.Bundle;
import android.os.IBinder;
import android.os.Looper;
import android.os.PowerManager;
import android.os.SystemClock;
import androidx.core.app.NotificationCompat;
import androidx.core.content.ContextCompat;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Service au premier plan qui capte le GPS et envoie lui-même les positions au serveur,
 * indépendamment de la page web : écran éteint, app en arrière-plan ou fermée.
 */
public class TrackerService extends Service implements LocationListener {
    static final String PREFS = "ryan_tracker";
    static final String KEY_ACTIVE = "active";
    static final String KEY_TOKEN = "token";
    static final String KEY_VEHICLE = "vehicleId";
    static final String KEY_BASE_URL = "baseUrl";
    static final String KEY_STATUS = "status";
    static final String KEY_MOVING = "movingSec";
    static final String KEY_IDLE = "idleSec";
    static final String KEY_PENDING = "pending";
    static final String KEY_LAST = "last";
    private static final String CHANNEL_ID = "ryan_tracker";
    private static final int NOTIF_ID = 4201;
    private static final int MAX_PENDING = 2000;

    private static final Object QUEUE_LOCK = new Object();

    private LocationManager locationManager;
    private PowerManager.WakeLock wakeLock;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private long lastSentElapsed = 0;
    private boolean stopping = false;

    static void start(Context context) {
        Intent intent = new Intent(context, TrackerService.class);
        ContextCompat.startForegroundService(context, intent);
    }

    static void stop(Context context) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS, MODE_PRIVATE);
        prefs.edit().putBoolean(KEY_ACTIVE, false).apply();
        context.stopService(new Intent(context, TrackerService.class));
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        if (!prefs.getBoolean(KEY_ACTIVE, false)) {
            stopping = true;
            stopSelf();
            return START_NOT_STICKY;
        }
        startInForeground();
        acquireWakeLock();
        registerLocationUpdates();
        executor.execute(this::flushQueue);
        return START_STICKY;
    }

    private void startInForeground() {
        NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (Build.VERSION.SDK_INT >= 26 && nm != null) {
            NotificationChannel channel = new NotificationChannel(CHANNEL_ID, "Suivi GPS", NotificationManager.IMPORTANCE_LOW);
            nm.createNotificationChannel(channel);
        }
        Intent open = new Intent(this, MainActivity.class);
        PendingIntent pi = PendingIntent.getActivity(this, 0, open, PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
        Notification notification = new NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("RyanERP")
            .setContentText("Suivi GPS actif")
            .setSmallIcon(getApplicationInfo().icon)
            .setOngoing(true)
            .setContentIntent(pi)
            .build();
        if (Build.VERSION.SDK_INT >= 29) {
            startForeground(NOTIF_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_LOCATION);
        } else {
            startForeground(NOTIF_ID, notification);
        }
    }

    private void acquireWakeLock() {
        if (wakeLock != null && wakeLock.isHeld()) return;
        PowerManager pm = (PowerManager) getSystemService(POWER_SERVICE);
        if (pm == null) return;
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "RyanERP:Tracker");
        wakeLock.setReferenceCounted(false);
        wakeLock.acquire();
    }

    private void registerLocationUpdates() {
        locationManager = (LocationManager) getSystemService(LOCATION_SERVICE);
        if (locationManager == null) return;
        try {
            locationManager.removeUpdates(this);
            if (locationManager.isProviderEnabled(LocationManager.GPS_PROVIDER)) {
                locationManager.requestLocationUpdates(LocationManager.GPS_PROVIDER, 5000, 0, this, Looper.getMainLooper());
            }
            if (locationManager.isProviderEnabled(LocationManager.NETWORK_PROVIDER)) {
                locationManager.requestLocationUpdates(LocationManager.NETWORK_PROVIDER, 15000, 0, this, Looper.getMainLooper());
            }
        } catch (SecurityException ignored) {
            // Permission de localisation retirée : le service reste actif et réessaiera au prochain redémarrage.
        }
    }

    @Override
    public void onLocationChanged(Location location) {
        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        double speedKph = location.hasSpeed() ? location.getSpeed() * 3.6 : 0;
        long intervalMs = (speedKph >= 5 ? prefs.getInt(KEY_MOVING, 20) : prefs.getInt(KEY_IDLE, 120)) * 1000L;
        long now = SystemClock.elapsedRealtime();
        boolean first = lastSentElapsed == 0;
        if (!first && now - lastSentElapsed < intervalMs) return;
        lastSentElapsed = now;

        try {
            JSONObject payload = new JSONObject();
            payload.put("latitude", location.getLatitude());
            payload.put("longitude", location.getLongitude());
            payload.put("speedKph", Math.max(speedKph, 0));
            payload.put("heading", location.hasBearing() ? Math.max(location.getBearing(), 0) : 0);
            payload.put("accuracyMeters", location.hasAccuracy() ? location.getAccuracy() : 0);
            payload.put("status", prefs.getString(KEY_STATUS, "online"));
            payload.put("source", "mobile_app");
            payload.put("note", "Tracker smartphone (service natif)");
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            payload.put("recordedAt", fmt.format(new Date(location.getTime() > 0 ? location.getTime() : System.currentTimeMillis())));
            int vehicleId = prefs.getInt(KEY_VEHICLE, 0);
            if (vehicleId > 0) payload.put("vehicleIdHint", vehicleId);

            prefs.edit().putString(KEY_LAST, payload.toString()).apply();
            synchronized (QUEUE_LOCK) {
                JSONArray queue = readQueue(prefs);
                queue.put(payload);
                writeQueue(prefs, queue);
            }
        } catch (Exception ignored) {
            return;
        }
        executor.execute(this::flushQueue);
    }

    private static JSONArray readQueue(SharedPreferences prefs) {
        try {
            return new JSONArray(prefs.getString(KEY_PENDING, "[]"));
        } catch (Exception e) {
            return new JSONArray();
        }
    }

    private static void writeQueue(SharedPreferences prefs, JSONArray queue) {
        if (queue.length() > MAX_PENDING) {
            JSONArray trimmed = new JSONArray();
            for (int i = queue.length() - MAX_PENDING; i < queue.length(); i++) trimmed.put(queue.opt(i));
            queue = trimmed;
        }
        prefs.edit().putString(KEY_PENDING, queue.toString()).apply();
    }

    private void flushQueue() {
        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        String token = prefs.getString(KEY_TOKEN, "");
        String baseUrl = prefs.getString(KEY_BASE_URL, "");
        if (token.isEmpty() || baseUrl.isEmpty()) return;

        while (true) {
            JSONObject next;
            synchronized (QUEUE_LOCK) {
                JSONArray queue = readQueue(prefs);
                if (queue.length() == 0) return;
                next = queue.optJSONObject(0);
            }
            int code = next == null ? 400 : post(baseUrl, token, next);
            if (code < 0 || code >= 500) return;
            synchronized (QUEUE_LOCK) {
                JSONArray queue = readQueue(prefs);
                JSONArray rest = new JSONArray();
                for (int i = 1; i < queue.length(); i++) rest.put(queue.opt(i));
                writeQueue(prefs, rest);
            }
        }
    }

    private int post(String baseUrl, String token, JSONObject payload) {
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(baseUrl + "/api/gps/ingest").openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(15000);
            conn.setReadTimeout(20000);
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("x-device-token", token);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
            }
            return conn.getResponseCode();
        } catch (Exception e) {
            return -1;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        scheduleRestart(this);
        super.onTaskRemoved(rootIntent);
    }

    @Override
    public void onDestroy() {
        if (locationManager != null) locationManager.removeUpdates(this);
        if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        executor.shutdown();
        boolean active = getSharedPreferences(PREFS, MODE_PRIVATE).getBoolean(KEY_ACTIVE, false);
        if (active && !stopping) scheduleRestart(this);
        super.onDestroy();
    }

    static void scheduleRestart(Context context) {
        try {
            AlarmManager am = (AlarmManager) context.getSystemService(ALARM_SERVICE);
            if (am == null) return;
            Intent intent = new Intent(context, BootReceiver.class).setAction(BootReceiver.ACTION_RESTART);
            PendingIntent pi = PendingIntent.getBroadcast(context, 1, intent, PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT);
            am.set(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime() + 3000, pi);
        } catch (Exception ignored) {
        }
    }

    @Override public void onProviderEnabled(String provider) { registerLocationUpdates(); }
    @Override public void onProviderDisabled(String provider) { }
    @Override public void onStatusChanged(String provider, int status, Bundle extras) { }
}
