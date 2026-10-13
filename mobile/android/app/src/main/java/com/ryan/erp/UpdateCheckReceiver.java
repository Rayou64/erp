package com.ryan.erp;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;

public class UpdateCheckReceiver extends BroadcastReceiver {
    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent == null || !AppUpdateManager.ACTION_CHECK.equals(intent.getAction())) return;
        PendingResult pendingResult = goAsync();
        AppUpdateManager.checkForUpdate(context, pendingResult::finish);
    }
}
