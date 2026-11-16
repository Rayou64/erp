package com.ryan.erp;

import android.Manifest;
import android.content.Intent;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.PowerManager;
import android.provider.Settings;
import androidx.annotation.NonNull;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;
import android.content.pm.PackageManager;
import java.util.ArrayList;
import com.getcapacitor.BridgeActivity;
import com.getcapacitor.CapConfig;
import android.webkit.JavascriptInterface;
import android.webkit.WebView;

public class MainActivity extends BridgeActivity {
    private static final int REQ_BASE = 7001;
    private static final int REQ_BACKGROUND = 7002;
    static final int REQ_NOTIFICATIONS = 7003;
    private static final String LOGIN_URL = "https://ryanerp-hn5zd.ondigitalocean.app/index.html?mobile=1";
    static final String MAIL_LOGIN_URL = "https://ryanerp-hn5zd.ondigitalocean.app/index.html?mail=1";
    private boolean firstStart = true;

    private boolean granted(String permission) {
        return ContextCompat.checkSelfPermission(this, permission) == PackageManager.PERMISSION_GRANTED;
    }

    @Override
    protected void load() {
        WebView webView = findViewById(com.getcapacitor.android.R.id.webview);
        if (BuildConfig.MESSAGING_ONLY) {
            webView.addJavascriptInterface(new MessagingIdentity(), "RyanNative");
        } else {
            webView.addJavascriptInterface(new NativeBridge(this), "RyanNative");
        }
        super.load();
        if (BuildConfig.MESSAGING_ONLY) {
            webView.getSettings().setGeolocationEnabled(false);
            getBridge().setWebViewClient(new MessagingWebViewClient(getBridge()));
        }
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        if (BuildConfig.MESSAGING_ONLY) {
            config = new CapConfig.Builder(this)
                .setServerUrl(MAIL_LOGIN_URL)
                .setAllowNavigation(new String[] { "ryanerp-hn5zd.ondigitalocean.app" })
                .setBackgroundColor("#060c18")
                .create();
        } else {
            registerPlugin(NativeTrackerPlugin.class);
        }
        super.onCreate(savedInstanceState);
        if (!BuildConfig.MESSAGING_ONLY) {
            if (!isMailIntent(getIntent())) showLogin();
            AppUpdateManager.schedule(this);
            AppUpdateManager.promptFromIntent(this, getIntent());
        }
        requestNotificationPermission();
    }

    public static final class MessagingIdentity {
        @JavascriptInterface
        public boolean isMessagingOnly() {
            return true;
        }
    }

    private boolean isMailIntent(Intent intent) {
        return intent != null && intent.hasExtra("google.message_id");
    }

    private void requestNotificationPermission() {
        if (Build.VERSION.SDK_INT >= 33 && !granted(Manifest.permission.POST_NOTIFICATIONS)) {
            ActivityCompat.requestPermissions(
                this,
                new String[] { Manifest.permission.POST_NOTIFICATIONS },
                REQ_NOTIFICATIONS
            );
            return;
        }
        checkForUpdateAndRequestLocation();
    }

    private void checkForUpdateAndRequestLocation() {
        if (BuildConfig.MESSAGING_ONLY) return;
        AppUpdateManager.checkForUpdate(this, null);
        requestLocationPermissions();
    }

    private void requestLocationPermissions() {
        ArrayList<String> missing = new ArrayList<>();
        String[] wanted = new String[] {
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION
        };
        for (String p : wanted) {
            if (!granted(p)) missing.add(p);
        }
        if (!missing.isEmpty()) {
            ActivityCompat.requestPermissions(this, missing.toArray(new String[0]), REQ_BASE);
        } else {
            requestBackgroundLocation();
        }
    }

    @Override
    public void onStart() {
        super.onStart();
        if (firstStart) {
            firstStart = false;
            return;
        }
        if (BuildConfig.MESSAGING_ONLY || isMailIntent(getIntent())) return;
        showLogin();
        if (Build.VERSION.SDK_INT < 33 || granted(Manifest.permission.POST_NOTIFICATIONS)) {
            AppUpdateManager.checkForUpdate(this, null);
        }
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        if (!BuildConfig.MESSAGING_ONLY) {
            if (!isMailIntent(intent)) showLogin();
            AppUpdateManager.promptFromIntent(this, intent);
        }
    }

    private void showLogin() {
        if (getBridge() == null || getBridge().getWebView() == null) return;
        getBridge().getWebView().loadUrl(LOGIN_URL);
    }

    private void notifyNotificationPermissionChanged() {
        if (getBridge() != null) getBridge().triggerWindowJSEvent("erp-native-notification-permission");
    }

    @Override
    public void onResume() {
        super.onResume();
        notifyNotificationPermissionChanged();
        if (!BuildConfig.MESSAGING_ONLY) AppUpdateManager.resumePendingInstall(this);
    }

    // Android demande la localisation « Toujours » dans une demande séparée, après la localisation normale.
    private void requestBackgroundLocation() {
        if (Build.VERSION.SDK_INT >= 29
            && granted(Manifest.permission.ACCESS_FINE_LOCATION)
            && !granted(Manifest.permission.ACCESS_BACKGROUND_LOCATION)) {
            ActivityCompat.requestPermissions(this, new String[] { Manifest.permission.ACCESS_BACKGROUND_LOCATION }, REQ_BACKGROUND);
        } else {
            requestIgnoreBatteryOptimizations();
        }
    }

    private void requestIgnoreBatteryOptimizations() {
        if (Build.VERSION.SDK_INT < 23) return;
        PowerManager pm = (PowerManager) getSystemService(POWER_SERVICE);
        if (pm == null || pm.isIgnoringBatteryOptimizations(getPackageName())) return;
        try {
            Intent intent = new Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS);
            intent.setData(Uri.parse("package:" + getPackageName()));
            startActivity(intent);
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQ_NOTIFICATIONS) {
            notifyNotificationPermissionChanged();
            checkForUpdateAndRequestLocation();
        } else if (requestCode == REQ_BASE) {
            requestBackgroundLocation();
        } else if (requestCode == REQ_BACKGROUND) {
            requestIgnoreBatteryOptimizations();
        }
    }
}