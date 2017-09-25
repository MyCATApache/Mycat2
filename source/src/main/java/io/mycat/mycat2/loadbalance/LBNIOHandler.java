package io.mycat.mycat2.loadbalance;

import io.mycat.mycat2.beans.conf.BalancerConfig;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.NIOAcceptor;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.MyCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * Created by ynfeng on 2017/9/13.
 */
public class LBNIOHandler implements NIOHandler<LBSession> {
    private final static Logger logger = LoggerFactory.getLogger(LBNIOHandler.class);

    @Override
    public void onConnect(SelectionKey curKey, LBSession session, boolean success, String msg) throws IOException {
        ProxyRuntime runtime = ProxyRuntime.INSTANCE;
        MyCluster cluster = runtime.getMyCLuster();
        BalancerConfig balancerConfig = runtime.getConfig().getConfig(ConfigEnum.BALANCER);
        LoadBalanceStrategy loadBalanceStrategy =
                LBStrategyConfig.getStrategy(balancerConfig.getBalancer().getStrategy());
        ClusterNode clusterNode = loadBalanceStrategy.getNode(cluster.allNodes.values(), null);
        connectToRemoteMycat(clusterNode.ip, clusterNode.proxyPort, runtime.getAcceptor().getSelector(), session);
    }

    private void connectToRemoteMycat(String ip, int port, Selector selector, LBSession lbSession) throws IOException {
        logger.info("load balancer dispatch connection to {}:{}", ip, port);
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
