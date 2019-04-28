package io.mycat.proxy;

import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.session.Session;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ThreadLocalRandom;

public class NIOAcceptor extends ProxyReactorThread<Session> {
    public NIOAcceptor(BufferPool bufPool) throws IOException {
        super(bufPool, null);
    }

    protected void processAcceptKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        ServerSocketChannel serverSocket = (ServerSocketChannel) curKey.channel();
        // 接收通道，设置为非阻塞模式
        final SocketChannel socketChannel = serverSocket.accept();
        socketChannel.configureBlocking(false);
        logger.info("new Client connected: " + socketChannel);
        // Mycat fontchannel connect
        accept(reactorEnv, socketChannel);

    }

    private void accept(ReactorEnv reactorEnv, SocketChannel socketChannel) throws IOException {
        // 找到一个可用的NIO Reactor Thread，交付托管
        MycatReactorThread[] reactorThreads = MycatRuntime.INSTANCE.getMycatReactorThreads();
        MycatReactorThread nioReactor = reactorThreads[ThreadLocalRandom.current().nextInt(reactorThreads.length)];
        // 将通道注册到reactor对象上
        nioReactor.acceptNewSocketChannel(null, socketChannel);
    }

    public boolean startServerChannel(String ip, int port) throws IOException {
        openServerChannel(selector, ip, port);
        return true;
    }
    @SuppressWarnings("unchecked")
    protected void processConnectKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        // only from cluster server socket
        SocketChannel curChannel = (SocketChannel) curKey.channel();
        Object obj = curKey.attachment();
        try {
            if (curChannel.finishConnect()) {
                throw new RuntimeException("");
            }
        } catch (ConnectException ex) {
            logger.warn("connect failed " + curChannel + " reason:" + ex);

        }
    }

    @SuppressWarnings("unchecked")
    protected void processReadKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        // only from cluster server socket
        Session session = (Session) curKey.attachment();
        session.getCurNIOHandler().onSocketRead(session);
    }

    protected void processWriteKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        // only from cluster server socket
        Session session = (Session) curKey.attachment();
        session.getCurNIOHandler().onSocketWrite(session);
    }

    private void openServerChannel(Selector selector, String bindIp, int bindPort)
            throws IOException {
        final ServerSocketChannel serverChannel = ServerSocketChannel.open();
        final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
        serverChannel.bind(isa);
        serverChannel.configureBlocking(false);
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public Selector getSelector() {
        return this.selector;
    }
}
