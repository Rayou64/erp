package com.ryan.erp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Looper;
import android.webkit.ServiceWorkerClient;
import android.webkit.ServiceWorkerController;
import android.webkit.ServiceWorkerWebSettings;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import com.getcapacitor.BridgeWebViewClient;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.Robolectric;
import org.robolectric.RobolectricTestRunner;
import org.robolectric.RuntimeEnvironment;
import org.robolectric.Shadows;
import org.robolectric.android.controller.ActivityController;
import org.robolectric.annotation.Config;
import org.robolectric.annotation.GraphicsMode;
import org.robolectric.annotation.Implementation;
import org.robolectric.annotation.Implements;
import org.robolectric.annotation.LooperMode;
import org.robolectric.shadows.ShadowWebView;

@RunWith(RobolectricTestRunner.class)
@Config(sdk = {24, 28, 33, 35}, shadows = MainActivityTest.ServiceWorkerControllerShadow.class)
@GraphicsMode(GraphicsMode.Mode.LEGACY)
@LooperMode(LooperMode.Mode.PAUSED)
public class MainActivityTest {
    @Before
    public void installWebViewProvider() {
        PackageInfo provider = new PackageInfo();
        provider.packageName = "com.google.android.webview";
        provider.versionName = "140.0.0.0";
        ShadowWebView.setCurrentWebViewPackage(provider);
    }

    @Test
    public void launcherCreatesBridgeWithoutCrashing() {
        try (ActivityController<MainActivity> controller = Robolectric.buildActivity(MainActivity.class).setup()) {
            MainActivity activity = controller.get();
            assertNotNull(activity.getBridge());
            assertNotNull(activity.getBridge().getWebView());
            assertNotNull(activity.getDrawable(R.drawable.launch_background));
            assertFalse(activity.isFinishing());
            Object identity = Shadows.shadowOf(activity.getBridge().getWebView()).getJavascriptInterface("RyanNative");
            if (BuildConfig.MESSAGING_ONLY) {
                assertTrue(activity.getBridge().getAppUrl().endsWith("/index.html?mail=1"));
                assertTrue(identity instanceof MainActivity.MessagingIdentity);
                assertNull(activity.getBridge().getPlugin("NativeTracker"));
                assertNull(activity.getBridge().getPlugin("BackgroundGeolocation"));
            } else {
                assertTrue(identity instanceof NativeBridge);
                assertNotNull(activity.getBridge().getPlugin("NativeTracker"));
                assertNotNull(activity.getBridge().getPlugin("BackgroundGeolocation"));
            }
            assertNotNull(activity.getBridge().getPlugin("PushNotifications"));
            assertNotNull(activity.getBridge().getPlugin("LocalNotifications"));
        }
    }

    @Test
    @Config(sdk = {24, 35})
    public void navigationMatchesTheApplicationFlavorAndPreservesMailAttachments() {
        try (ActivityController<MainActivity> controller = Robolectric.buildActivity(MainActivity.class).setup()) {
            MainActivity activity = controller.get();
            WebView view = activity.getBridge().getWebView();
            BridgeWebViewClient client = activity.getBridge().getWebViewClient();
            String origin = Uri.parse(MainActivity.MAIL_LOGIN_URL).buildUpon().path(null).clearQuery().fragment(null).build().toString();
            assertEquals(BuildConfig.MESSAGING_ONLY, client.shouldOverrideUrlLoading(view, request(origin + "/erp.html")));
            assertFalse(client.shouldOverrideUrlLoading(view, request(MainActivity.MAIL_LOGIN_URL)));
            assertFalse(client.shouldOverrideUrlLoading(view, request(origin + "/messaging.html?conversation=17")));
            assertFalse(client.shouldOverrideUrlLoading(view, request(origin + "/api/internal-mail/attachments/5/download")));
            assertFalse(client.shouldOverrideUrlLoading(view, request("blob:" + origin + "/mail-attachment")));
            if (BuildConfig.MESSAGING_ONLY) {
                assertTrue(client instanceof MessagingWebViewClient);
                assertTrue(client.shouldOverrideUrlLoading(view, request(origin + "/api/users")));
                assertTrue(client.shouldOverrideUrlLoading(view, request(origin.replace("https:", "http:") + "/messaging.html")));
                assertTrue(client.shouldOverrideUrlLoading(view, request("blob:https://unrelated.example/mail-attachment")));
                assertTrue(client.shouldOverrideUrlLoading(view, origin + "/erp.html?openModule=users"));
                client.onPageStarted(view, origin + "/erp.html", null);
                assertEquals(MainActivity.MAIL_LOGIN_URL, Shadows.shadowOf(view).getLastLoadedUrl());
                client.onPageFinished(view, MainActivity.MAIL_LOGIN_URL);
            } else {
                assertFalse(client instanceof MessagingWebViewClient);
                assertFalse(client.shouldOverrideUrlLoading(view, request(origin + "/erp.html?openModule=users")));
            }
        }
    }

    @Test
    @Config(sdk = {33, 35})
    public void androidPermissionResultNotifiesTheAutomaticWebRegistration() {
        try (ActivityController<MainActivity> controller = Robolectric.buildActivity(MainActivity.class).setup()) {
            MainActivity activity = controller.get();
            WebView view = activity.getBridge().getWebView();
            Shadows.shadowOf(Looper.getMainLooper()).idle();
            Shadows.shadowOf(RuntimeEnvironment.getApplication()).grantPermissions(Manifest.permission.POST_NOTIFICATIONS);
            view.evaluateJavascript("window.permissionCallbackMarker = true;", null);
            activity.onRequestPermissionsResult(
                MainActivity.REQ_NOTIFICATIONS,
                new String[] { Manifest.permission.POST_NOTIFICATIONS },
                new int[] { PackageManager.PERMISSION_GRANTED }
            );
            Shadows.shadowOf(Looper.getMainLooper()).idle();
            assertTrue(Shadows.shadowOf(view).getLastEvaluatedJavascript().contains("erp-native-notification-permission"));
        }
    }

    @Test
    @Config(sdk = {24, 35})
    public void coldLaunchRequiresLoginButResumeAndNotificationDoNotRestartLogin() {
        Intent coldNotification = new Intent(RuntimeEnvironment.getApplication(), MainActivity.class)
            .putExtra(MailNotifications.EXTRA_URL, "/messaging.html?conversation=42");
        try (ActivityController<MainActivity> controller = Robolectric.buildActivity(MainActivity.class, coldNotification).setup()) {
            MainActivity activity = controller.get();
            WebView view = activity.getBridge().getWebView();
            String login = Shadows.shadowOf(view).getLastLoadedUrl();
            assertTrue(login.contains("mobile=1"));
            assertTrue(login.contains("conversation=42"));
            String inbox = "https://ryanerp-hn5zd.ondigitalocean.app/messaging.html";
            view.loadUrl(inbox);
            controller.pause().stop().start().resume();
            assertEquals(inbox, Shadows.shadowOf(view).getLastLoadedUrl());
            activity.onNewIntent(new Intent(activity, MainActivity.class));
            assertEquals(inbox, Shadows.shadowOf(view).getLastLoadedUrl());
            activity.onNewIntent(new Intent(activity, MainActivity.class)
                .putExtra(MailNotifications.EXTRA_URL, "/messaging.html?conversation=17"));
            assertEquals(inbox + "?conversation=17", Shadows.shadowOf(view).getLastLoadedUrl());
        }
    }

    @Test
    @Config(sdk = {35}, qualifiers = "night")
    public void safariPreservesTheMailLightPaletteEvenWithAndroidDarkMode() {
        org.junit.Assume.assumeTrue(BuildConfig.MESSAGING_ONLY);
        android.content.res.Resources.Theme theme = RuntimeEnvironment.getApplication().getResources().newTheme();
        theme.applyStyle(R.style.AppTheme_NoActionBar, true);
        android.util.TypedValue value = new android.util.TypedValue();
        assertTrue(theme.resolveAttribute(android.R.attr.isLightTheme, value, true));
        assertTrue(value.data != 0);
        assertTrue(theme.resolveAttribute(android.R.attr.forceDarkAllowed, value, true));
        assertEquals(0, value.data);
    }

    private static WebResourceRequest request(String url) {
        return new WebResourceRequest() {
            @Override
            public Uri getUrl() {
                return Uri.parse(url);
            }

            @Override
            public boolean isForMainFrame() {
                return true;
            }

            @Override
            public boolean isRedirect() {
                return false;
            }

            @Override
            public boolean hasGesture() {
                return true;
            }

            @Override
            public String getMethod() {
                return "GET";
            }

            @Override
            public Map<String, String> getRequestHeaders() {
                return Collections.emptyMap();
            }
        };
    }

    @Implements(ServiceWorkerController.class)
    public static class ServiceWorkerControllerShadow {
        @Implementation
        protected static ServiceWorkerController getInstance() {
            return new ServiceWorkerController() {
                @Override
                public ServiceWorkerWebSettings getServiceWorkerWebSettings() {
                    throw new UnsupportedOperationException("Startup does not access service worker settings");
                }

                @Override
                public void setServiceWorkerClient(ServiceWorkerClient client) {
                    assertNotNull(client);
                }
            };
        }
    }
}
