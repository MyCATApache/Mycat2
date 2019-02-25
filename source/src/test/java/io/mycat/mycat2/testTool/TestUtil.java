package io.mycat.mycat2.testTool;

import io.mycat.mysql.packet.*;
import io.mycat.proxy.ProxyBuffer;
import sun.nio.ch.SelectorProviderImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;

/**
 * cjw
 * 294712221@qq.com
 */
public class TestUtil {
    public static SelectorProvider mockSelectorProvider(){
        SelectorProviderImpl s = new SelectorProviderImpl(){
            @Override
            public AbstractSelector openSelector() throws IOException {
                return null;
            }
        };
        return s;
    }

    public static SocketChannel mockSocketChannel(SelectorProvider selectorProvider,ByteBuffer readBuffer,ByteBuffer writeBuffer){
        return new MockSocketChannel(selectorProvider,readBuffer,writeBuffer);
    }

    public static Selector mockSelector(SelectorProvider selectorProvider ){
        Selector mock = new AbstractSelector(selectorProvider) {
            @Override
            protected void implCloseSelector() throws IOException {

            }

            @Override
            protected SelectionKey register(AbstractSelectableChannel ch, int ops, Object att) {
                return null;
            }

            @Override
            public Set<SelectionKey> keys() {
                return null;
            }

            @Override
            public Set<SelectionKey> selectedKeys() {
                return null;
            }

            @Override
            public int selectNow() throws IOException {
                return 0;
            }

            @Override
            public int select(long timeout) throws IOException {
                return 0;
            }

            @Override
            public int select() throws IOException {
                return 0;
            }

            @Override
            public Selector wakeup() {
                return null;
            }
        };
        return mock;
    }
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
    public static ErrorPacket errPacket(int packetId) {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = (byte) packetId;
        eofPacket.message = "";
        return eofPacket;
    }
    public static void  anyPacket(int payloadLength,int packetId,ProxyBuffer buffer) {
        buffer.writeFixInt(3, payloadLength);
        buffer.writeByte((byte) packetId);
    }
    public static ProxyBuffer errBuffer() {
        ErrorPacket eofPacket = new ErrorPacket();
        eofPacket.packetId = 12;
        eofPacket.message = "";
        ProxyBuffer buffer = TestUtil.exampleBuffer();
        eofPacket.write(buffer);
        return buffer;
    }
    public static ProxyBuffer fieldCount(int count) {
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        headerPacket.fieldCount = count;
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
