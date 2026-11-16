package com.ryan.erp;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import android.content.pm.PackageInfo;
import android.webkit.ServiceWorkerClient;
import android.webkit.ServiceWorkerController;
import android.webkit.ServiceWorkerWebSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.Robolectric;
import org.robolectric.RobolectricTestRunner;
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
            if (BuildConfig.MESSAGING_ONLY) {
                assertTrue(activity.getBridge().getAppUrl().endsWith("/index.html?mail=1"));
            }
        }
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
