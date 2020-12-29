package io.mycat.proxy.handler.front;

import org.jetbrains.annotations.Nullable;

import java.net.SocketAddress;

public class SocketAddressUtil {
    @Nullable
    public static String simplySocketAddress(SocketAddress remoteSocketAddress) {
        if (remoteSocketAddress == null) {
            return null;
        }
        String string = remoteSocketAddress.toString();
        return simplySocketAddress(string);
    }

    public static String simplySocketAddress(String string) {
        if (string != null) {
            if (string.startsWith("/")) {
                string = string.substring(1);
            }
        }
        return string;
    }

}