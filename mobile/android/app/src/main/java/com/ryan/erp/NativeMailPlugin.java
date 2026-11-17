package com.ryan.erp;

import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;

@CapacitorPlugin(name = "NativeMail")
public class NativeMailPlugin extends Plugin {
    @PluginMethod
    public void setAccount(PluginCall call) {
        int userId = call.getInt("userId", 0);
        if (userId <= 0) {
            call.reject("Compte de notifications invalide.");
            return;
        }
        MailNotifications.setAccount(getContext(), userId);
        call.resolve();
    }

    @PluginMethod
    public void clearAccount(PluginCall call) {
        MailNotifications.setAccount(getContext(), 0);
        call.resolve();
    }
}
