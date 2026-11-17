package com.ryan.erp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationManager;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ApplicationInfo;
import android.content.pm.PackageInfo;
import android.os.Build;
import androidx.core.app.NotificationCompat;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.RuntimeEnvironment;
import org.robolectric.Shadows;
import org.robolectric.annotation.Config;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {24, 35})
public class MailNotificationsTest {
    private Context context;
    private NotificationManager manager;

    @Before
    public void prepareAccount() {
        context = RuntimeEnvironment.getApplication();
        manager = context.getSystemService(NotificationManager.class);
        manager.cancelAll();
        if (Build.VERSION.SDK_INT >= 33) Shadows.shadowOf(RuntimeEnvironment.getApplication()).grantPermissions(Manifest.permission.POST_NOTIFICATIONS);
        MailNotifications.setAccount(context, 12);
    }

    private Map<String, String> message(String conversation, String tag) {
        Map<String, String> data = new HashMap<>();
        data.put("mailStyle", "1");
        data.put("userId", "12");
        data.put("url", "/messaging.html?conversation=" + conversation);
        data.put("tag", tag);
        data.put("senderName", "Alice Martin");
        data.put("subject", "Reunion chantier");
        data.put("preview", "Bonjour, les documents sont disponibles.");
        return data;
    }

    @Test
    public void messagesStackByConversationWithoutRepeatingTheSameMessageAndKeepTheirDeepLink() throws Exception {
        MailNotifications.show(context, message("42", "mail:1"), 1000);
        MailNotifications.show(context, message("42", "mail:2"), 2000);
        MailNotifications.show(context, message("42", "mail:2"), 2000);
        assertEquals(1, manager.getActiveNotifications().length);
        Notification notification = manager.getActiveNotifications()[0].getNotification();
        NotificationCompat.MessagingStyle style = NotificationCompat.MessagingStyle.extractMessagingStyleFromNotification(notification);
        assertNotNull(style);
        assertEquals(2, style.getMessages().size());
        assertEquals("Reunion chantier", style.getConversationTitle().toString());
        assertEquals("Alice Martin", style.getMessages().get(0).getPerson().getName().toString());
        assertTrue(style.getMessages().get(0).getText().toString().contains("documents"));
        Intent intent = Shadows.shadowOf(notification.contentIntent).getSavedIntent();
        assertEquals("/messaging.html?conversation=42", intent.getStringExtra(MailNotifications.EXTRA_URL));
        MailNotifications.show(context, message("43", "mail:3"), 3000);
        assertEquals(2, manager.getActiveNotifications().length);
        MailNotifications.setAccount(context, 0);
        assertEquals(0, manager.getActiveNotifications().length);
        MailNotifications.show(context, message("42", "mail:4"), 4000);
        assertEquals(0, manager.getActiveNotifications().length);
    }

    @Test
    public void safariInstallationOnlySuppressesErpOnThisDeviceAndUninstallRestoresIt() {
        MailNotifications.show(context, message("42", "mail:1"), 1000);
        assertEquals(1, manager.getActiveNotifications().length);
        PackageInfo safari = new PackageInfo();
        safari.packageName = MailNotifications.SAFARI_PACKAGE;
        safari.applicationInfo = new ApplicationInfo();
        safari.applicationInfo.packageName = safari.packageName;
        safari.applicationInfo.enabled = true;
        Shadows.shadowOf(context.getPackageManager()).installPackage(safari);
        MailNotifications.show(context, message("42", "mail:2"), 2000);
        assertEquals(BuildConfig.MESSAGING_ONLY ? 1 : 0, manager.getActiveNotifications().length);
        if (!BuildConfig.MESSAGING_ONLY) {
            Shadows.shadowOf(context.getPackageManager()).removePackage(safari.packageName);
            MailNotifications.show(context, message("42", "mail:3"), 3000);
            assertEquals(1, manager.getActiveNotifications().length);
        }
    }

    @Test
    public void wrongAccountDeniedPermissionAndUnsafeConversationNeverDisplayMessages() {
        Map<String, String> data = message("42", "mail:1");
        data.put("userId", "19");
        MailNotifications.show(context, data, 1000);
        assertEquals(0, manager.getActiveNotifications().length);
        assertNull(MailNotifications.mailPath("https://other.example/messaging.html?conversation=42"));
        assertNull(MailNotifications.mailPath("//other.example/messaging.html?conversation=42"));
        assertNull(MailNotifications.mailPath("/erp.html"));
        assertNull(MailNotifications.mailPath("/messaging.html?conversation=invalid"));
        if (Build.VERSION.SDK_INT >= 33) {
            Shadows.shadowOf(RuntimeEnvironment.getApplication()).denyPermissions(Manifest.permission.POST_NOTIFICATIONS);
            MailNotifications.show(context, message("42", "mail:2"), 2000);
            assertEquals(0, manager.getActiveNotifications().length);
        }
    }
}
