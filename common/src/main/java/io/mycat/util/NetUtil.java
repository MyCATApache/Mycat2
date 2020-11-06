package io.mycat.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class NetUtil {
    public static boolean isHostConnectable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port));
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}