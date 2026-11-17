package com.ryan.erp;

import android.util.Log;
import androidx.annotation.NonNull;
import com.capacitorjs.plugins.pushnotifications.MessagingService;
import com.google.firebase.messaging.RemoteMessage;

public class MailMessagingService extends MessagingService {
    @Override
    public void onMessageReceived(@NonNull RemoteMessage message) {
        if ("1".equals(message.getData().get("mailStyle"))) {
            try {
                MailNotifications.show(this, message.getData(), message.getSentTime());
            } catch (SecurityException error) {
                Log.e("native-mail", "Notification permission changed during delivery", error);
            }
        }
        super.onMessageReceived(message);
    }
}
