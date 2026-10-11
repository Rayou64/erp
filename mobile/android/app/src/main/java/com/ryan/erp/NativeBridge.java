package com.ryan.erp;

import android.content.Context;
import android.content.SharedPreferences;
import android.webkit.JavascriptInterface;
import org.json.JSONArray;
import org.json.JSONObject;

/** Pont JavaScript direct, utilisable par la page de suivi même chargée depuis le serveur distant. */
public class NativeBridge {
    private final Context context;

    NativeBridge(Context context) {
        this.context = context.getApplicationContext();
    }

    private SharedPreferences prefs() {
        return context.getSharedPreferences(TrackerService.PREFS, Context.MODE_PRIVATE);
    }

    @JavascriptInterface
    public String start(String token, String baseUrl, int vehicleId, String status, int movingSec, int idleSec) {
        if (token == null || token.isEmpty() || baseUrl == null || baseUrl.isEmpty()) return "token et baseUrl requis";
        prefs().edit()
            .putBoolean(TrackerService.KEY_ACTIVE, true)
            .putString(TrackerService.KEY_TOKEN, token)
            .putString(TrackerService.KEY_BASE_URL, baseUrl.replaceAll("/+$", ""))
            .putInt(TrackerService.KEY_VEHICLE, vehicleId)
            .putString(TrackerService.KEY_STATUS, status == null || status.isEmpty() ? "online" : status)
            .putInt(TrackerService.KEY_MOVING, Math.max(movingSec, 1))
            .putInt(TrackerService.KEY_IDLE, Math.max(idleSec, 1))
            .apply();
        try {
            TrackerService.start(context);
            return "";
        } catch (Exception e) {
            return "Impossible de démarrer le service : " + e.getMessage();
        }
    }

    @JavascriptInterface
    public void stop() {
        TrackerService.stop(context);
    }

    @JavascriptInterface
    public String getStatus() {
        SharedPreferences p = prefs();
        try {
            JSONObject res = new JSONObject();
            res.put("active", p.getBoolean(TrackerService.KEY_ACTIVE, false));
            res.put("vehicleId", p.getInt(TrackerService.KEY_VEHICLE, 0));
            res.put("pending", new JSONArray(p.getString(TrackerService.KEY_PENDING, "[]")).length());
            String last = p.getString(TrackerService.KEY_LAST, "");
            if (!last.isEmpty()) res.put("last", new JSONObject(last));
            return res.toString();
        } catch (Exception e) {
            return "{}";
        }
    }
}
