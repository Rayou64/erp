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

public class MainActivity extends BridgeActivity {
    private static final int REQ_BASE = 7001;
    private static final int REQ_BACKGROUND = 7002;
    private static final String LOGIN_URL = "https://ryanerp-hn5zd.ondigitalocean.app/index.html?mobile=1";
    private boolean firstStart = true;

    private boolean granted(String permission) {
        return ContextCompat.checkSelfPermission(this, permission) == PackageManager.PERMISSION_GRANTED;
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        registerPlugin(NativeTrackerPlugin.class);
        super.onCreate(savedInstanceState);
        getBridge().getWebView().addJavascriptInterface(new NativeBridge(this), "RyanNative");
        showLogin();
        AppUpdateManager.schedule(this);
        AppUpdateManager.checkForUpdate(this, null);
        AppUpdateManager.promptFromIntent(this, getIntent());
        ArrayList<String> missing = new ArrayList<>();
        String[] wanted = new String[] {
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION
        };
        for (String p : wanted) {
            if (!granted(p)) missing.add(p);
        }
        if (Build.VERSION.SDK_INT >= 33 && !granted(Manifest.permission.POST_NOTIFICATIONS)) {
            missing.add(Manifest.permission.POST_NOTIFICATIONS);
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
        showLogin();
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        showLogin();
        AppUpdateManager.promptFromIntent(this, intent);
    }

    private void showLogin() {
        if (getBridge() == null || getBridge().getWebView() == null) return;
        getBridge().getWebView().loadUrl(LOGIN_URL);
    }

    @Override
    public void onResume() {
        super.onResume();
        AppUpdateManager.resumePendingInstall(this);
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
        if (requestCode == REQ_BASE) {
            requestBackgroundLocation();
        } else if (requestCode == REQ_BACKGROUND) {
            requestIgnoreBatteryOptimizations();
        }
    }
}