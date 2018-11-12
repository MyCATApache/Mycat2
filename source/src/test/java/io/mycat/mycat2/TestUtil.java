package io.mycat.mycat2;

import io.mycat.mysql.packet.*;
import io.mycat.proxy.ProxyBuffer;

import java.nio.ByteBuffer;

/**
 * cjw
 * 294712221@qq.com
 */
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

    public static ProxyBuffer exampleBuffer() {
        ByteBuffer allocate = ByteBuffer.allocate(128);
        return new ProxyBuffer(allocate);
    }

    public static ProxyBuffer eof(int serverStatus) {
        EOFPacket eofPacket = new EOFPacket();
        ProxyBuffer buffer = exampleBuffer();
        eofPacket.status = serverStatus;
        eofPacket.write(buffer);
        return buffer;
    }

    public static ProxyBuffer ok(int serverStatus) {
        OKPacket okPacket = new OKPacket();
        ProxyBuffer buffer = exampleBuffer();
        okPacket.serverStatus = serverStatus;
        okPacket.write(buffer);
        return buffer;
    }

    public static ProxyBuffer err() {
        ErrorPacket eofPacket = new ErrorPacket();
        ProxyBuffer buffer = exampleBuffer();
        eofPacket.write(buffer);
        return buffer;
    }
    public static ProxyBuffer fieldCount() {
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        ProxyBuffer buffer = exampleBuffer();
        headerPacket.write(buffer);
        return buffer;
    }
    public static ProxyBuffer field() {
        FieldPacket fieldPacket = new FieldPacket();
        ProxyBuffer buffer = exampleBuffer();
        fieldPacket.write(buffer);
        return buffer;
    }

    public static ProxyBuffer row(int field) {
        RowDataPacket rowDataPacket = new RowDataPacket(field);
        ProxyBuffer buffer = exampleBuffer();
        rowDataPacket.write(buffer);
        return buffer;
    }
}
