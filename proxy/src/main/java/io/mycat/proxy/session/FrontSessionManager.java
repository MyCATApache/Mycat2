package io.mycat.proxy.session;

import io.mycat.proxy.buffer.BufferPool;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface FrontSessionManager<T extends Session> extends SessionManager<T>  {

    T acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool, Selector selector, SocketChannel socketChannel) throws IOException;
}
