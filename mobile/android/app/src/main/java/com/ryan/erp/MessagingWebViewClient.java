package com.ryan.erp;

import android.graphics.Bitmap;
import android.net.Uri;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import android.widget.Toast;
import com.getcapacitor.Bridge;
import com.getcapacitor.BridgeWebViewClient;

final class MessagingWebViewClient extends BridgeWebViewClient {
    private final Bridge bridge;
    private final String host = Uri.parse(MainActivity.MAIL_LOGIN_URL).getHost();
    private boolean clearInvalidHistory;

    MessagingWebViewClient(Bridge bridge) {
        super(bridge);
        this.bridge = bridge;
    }

    private boolean isServerOrigin(Uri url) {
        return "https".equalsIgnoreCase(url.getScheme())
            && host.equalsIgnoreCase(url.getHost())
            && url.getUserInfo() == null
            && (url.getPort() == -1 || url.getPort() == 443);
    }

    private boolean isMailPage(Uri url) {
        return isServerOrigin(url) && (
            "/".equals(url.getPath())
            || "/index.html".equals(url.getPath())
            || "/messaging.html".equals(url.getPath())
        );
    }

    private boolean isMailLocation(Uri url) {
        if (isMailPage(url)) return true;
        if ("blob".equals(url.getScheme())) {
            return isServerOrigin(Uri.parse(url.getSchemeSpecificPart()));
        }
        String path = url.getPath();
        return isServerOrigin(url) && path != null && path.matches(
            "/api/internal-mail/(attachments/[1-9][0-9]*/download|conversations/[1-9][0-9]*/attachments/download-all)"
        );
    }

    private void explainMailOnly(WebView view) {
        Toast.makeText(view.getContext(), "Portail Safari : messagerie uniquement.", Toast.LENGTH_SHORT).show();
    }

    private boolean navigate(WebView view, Uri url) {
        if (isMailLocation(url) || "about:blank".equals(url.toString())) return false;
        String scheme = url.getScheme();
        boolean externalWebsite = ("https".equalsIgnoreCase(scheme) || "http".equalsIgnoreCase(scheme))
            && !host.equalsIgnoreCase(url.getHost());
        if (externalWebsite || "mailto".equalsIgnoreCase(scheme) || "tel".equalsIgnoreCase(scheme)) {
            return bridge.launchIntent(url);
        }
        explainMailOnly(view);
        return true;
    }

    @Override
    public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
        return navigate(view, request.getUrl());
    }

    @Override
    public boolean shouldOverrideUrlLoading(WebView view, String url) {
        return navigate(view, Uri.parse(url));
    }

    @Override
    public void onPageStarted(WebView view, String url, Bitmap favicon) {
        Uri location = Uri.parse(url);
        if (!isMailLocation(location) && !"about:blank".equals(url) && !"chrome-error".equals(location.getScheme())) {
            clearInvalidHistory = true;
            view.stopLoading();
            explainMailOnly(view);
            view.loadUrl(MainActivity.MAIL_LOGIN_URL);
            return;
        }
        super.onPageStarted(view, url, favicon);
    }

    @Override
    public void onPageFinished(WebView view, String url) {
        super.onPageFinished(view, url);
        if (clearInvalidHistory && isMailPage(Uri.parse(url))) {
            view.clearHistory();
            clearInvalidHistory = false;
        }
    }
}
