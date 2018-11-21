package io.mycat.mycat2.handler;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.testTool.TestUtil;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.buffer.DirectByteBufferPool;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

public class BaseHandlerTest {

    @Test
    public void initHandlerTest() throws Exception {
        MainMycatNIOHandler mainMycatNIOHandler = new MainMycatNIOHandler();
        SelectorProvider selectorProvider = TestUtil.mockSelectorProvider();
        Selector selector = TestUtil.mockSelector(selectorProvider);
        ByteBuffer readBuffer = ByteBuffer.allocate(4096);
        ByteBuffer writeBuffer = ByteBuffer.allocate(4096);
        SocketChannel socketChannel = TestUtil.mockSocketChannel(selectorProvider, readBuffer, writeBuffer);
        BufferPool pool = new DirectByteBufferPool(8, (short) 8, 8);
        MycatSession session = new MycatSession(pool, selector, socketChannel);
        mainMycatNIOHandler.onSocketRead(session);
    }
}
