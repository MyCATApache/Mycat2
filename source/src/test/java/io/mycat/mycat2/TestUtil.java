package io.mycat.mycat2;

import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;

public class TestUtil {
    public static byte[] of(int... i) {
        byte[] bytes = new byte[i.length];
        int j = 0;
        for (int i1 : i) {
            bytes[j] = (byte) i1;
            j++;
        }
        return bytes;
    }

    public static ProxyBuffer ofBuffer(int... i) {
        ProxyBuffer proxyBuffer = new ProxyBuffer(ByteBuffer.wrap(of(i)));
        proxyBuffer.writeIndex = i.length;
        return proxyBuffer;
    }
}
