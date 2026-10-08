package com.ryan.erp;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;

/** Relance le suivi après un redémarrage du téléphone, une mise à jour de l'app ou un kill par le système. */
public class BootReceiver extends BroadcastReceiver {
    static final String ACTION_RESTART = "com.ryan.erp.RESTART_TRACKER";

    @Override
    public void onReceive(Context context, Intent intent) {
        boolean active = context.getSharedPreferences(TrackerService.PREFS, Context.MODE_PRIVATE)
            .getBoolean(TrackerService.KEY_ACTIVE, false);
        if (!active) return;
        try {
            TrackerService.start(context);
        } catch (Exception e) {
            TrackerService.scheduleRestart(context);
        }
    }
}
