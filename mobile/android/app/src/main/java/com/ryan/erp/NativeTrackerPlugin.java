package com.ryan.erp;

import android.content.Context;
import android.content.SharedPreferences;
import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;
import org.json.JSONArray;
import org.json.JSONObject;

@CapacitorPlugin(name = "NativeTracker")
public class NativeTrackerPlugin extends Plugin {

    private SharedPreferences prefs() {
        return getContext().getSharedPreferences(TrackerService.PREFS, Context.MODE_PRIVATE);
    }

    @PluginMethod
    public void start(PluginCall call) {
        String token = call.getString("token", "");
        String baseUrl = call.getString("baseUrl", "");
        if (token.isEmpty() || baseUrl.isEmpty()) {
            call.reject("token et baseUrl requis");
            return;
        }
        prefs().edit()
            .putBoolean(TrackerService.KEY_ACTIVE, true)
            .putString(TrackerService.KEY_TOKEN, token)
            .putString(TrackerService.KEY_BASE_URL, baseUrl.replaceAll("/+$", ""))
            .putInt(TrackerService.KEY_VEHICLE, call.getInt("vehicleId", 0))
            .putString(TrackerService.KEY_STATUS, call.getString("status", "online"))
            .putInt(TrackerService.KEY_MOVING, Math.max(call.getInt("movingSec", 20), 5))
            .putInt(TrackerService.KEY_IDLE, Math.max(call.getInt("idleSec", 120), 20))
            .apply();
        try {
            TrackerService.start(getContext());
            call.resolve();
        } catch (Exception e) {
            call.reject("Impossible de démarrer le service : " + e.getMessage());
        }
    }

    @PluginMethod
    public void stop(PluginCall call) {
        TrackerService.stop(getContext());
        call.resolve();
    }

    @PluginMethod
    public void getStatus(PluginCall call) {
        SharedPreferences p = prefs();
        JSObject res = new JSObject();
        res.put("active", p.getBoolean(TrackerService.KEY_ACTIVE, false));
        res.put("token", p.getString(TrackerService.KEY_TOKEN, ""));
        res.put("vehicleId", p.getInt(TrackerService.KEY_VEHICLE, 0));
        try {
            res.put("pending", new JSONArray(p.getString(TrackerService.KEY_PENDING, "[]")).length());
            String last = p.getString(TrackerService.KEY_LAST, "");
            if (!last.isEmpty()) res.put("last", new JSObject(new JSONObject(last).toString()));
        } catch (Exception ignored) {
        }
        call.resolve(res);
    }
}
