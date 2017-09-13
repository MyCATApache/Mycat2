package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.MyCluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Created by ynfeng on 2017/9/13.
 */
public class LBNIOHandler implements NIOHandler<LBSession> {
    @Override
    public void onConnect(SelectionKey curKey, LBSession session, boolean success, String msg) throws IOException {
        ProxyRuntime runtime = ProxyRuntime.INSTANCE;
//        MyCluster cluster = runtime.getMyCLuster();
//        ClusterNode clusterNode = runtime.getLoadBalanceStrategy().getNode(cluster.allNodes.values(), null);
        connectToRemoteMycat("127.0.0.1", 8066, runtime.getAcceptor().getSelector(), session);
    }

    private void connectToRemoteMycat(String ip, int port, Selector selector, LBSession lbSession) throws IOException {
        InetSocketAddress address = new InetSocketAddress(ip, port);
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(false);
        sc.register(selector, SelectionKey.OP_CONNECT, lbSession);
        sc.connect(address);
    }

    @Override
    public void onSocketRead(LBSession session) throws IOException {
        if (session.readFromChannel()) {
            ProxyBuffer curBuffer = session.proxyBuffer;
            curBuffer.flip();
            curBuffer.readIndex = curBuffer.writeIndex;
            session.giveupOwner(SelectionKey.OP_WRITE);
            session.getProxySession().writeToChannel();
        }
    }

    @Override
    public void onSocketWrite(LBSession session) throws IOException {
        session.writeToChannel();
    }

    @Override
    public void onWriteFinished(LBSession session) throws IOException {
        session.proxyBuffer.flip();
        session.change2ReadOpts();
    }

    @Override
    public void onSocketClosed(LBSession session, boolean normal) {

    }
}
