package io.mycat.proxy;

/**
 * NIO Acceptor ,只用来接受新连接的请求，也负责处理管理端口的报文
 *
 * @author wuzhihui
 */

import io.mycat.mycat2.beans.conf.BalancerConfig;
import io.mycat.mycat2.loadbalance.*;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.ClusterNode;
import io.mycat.proxy.man.DefaultAdminSessionManager;
import io.mycat.proxy.man.MyCluster.ClusterState;
import io.mycat.proxy.man.packet.NodeRegInfoPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final static Logger logger = LoggerFactory.getLogger(NIOAcceptor.class);
    protected SessionManager<AdminSession> adminSessionMan;

    private ServerSocketChannel proxyServerSocketChannel;
    private ServerSocketChannel clusterServerSocketChannel;
    private ServerSocketChannel loadBalanceServerSocketChannel;

    public NIOAcceptor(BufferPool bufferPool) throws IOException {
        super(bufferPool);
        this.setName("NIO-Acceptor");
    }

    public boolean startServerChannel(String ip, int port, ServerType serverType) throws IOException {
        final ServerSocketChannel serverChannel = getServerSocketChannel(serverType);
        if (serverChannel != null && serverChannel.isOpen()) {
            return false;
        }

        if (serverType == ServerType.CLUSTER) {
            adminSessionMan = new DefaultAdminSessionManager();
            ProxyRuntime.INSTANCE.setAdminSessionManager(adminSessionMan);
            logger.info("opend cluster conmunite port on {}:{}", ip, port);
        } else if (serverType == ServerType.LOAD_BALANCER) {
            logger.info("opend load balance conmunite port on {}:{}", ip, port);
            ProxyRuntime.INSTANCE.setProxySessionSessionManager(new ProxySessionManager());
            ProxyRuntime.INSTANCE.setLbSessionSessionManager(new LBSessionManager());
        }

        openServerChannel(selector, ip, port, serverType);
        return true;
    }

    private ServerSocketChannel getServerSocketChannel(ServerType serverType) {
        switch (serverType) {
            case MYCAT:
                return proxyServerSocketChannel;
            case CLUSTER:
                return clusterServerSocketChannel;
            case LOAD_BALANCER:
                return loadBalanceServerSocketChannel;
            default:
                throw new IllegalArgumentException("wrong server type.");
        }
    }

    public void stopServerChannel(boolean clusterServer) {
        ServerSocketChannel socketChannel = clusterServer ? clusterServerSocketChannel : proxyServerSocketChannel;
        if (socketChannel != null && socketChannel.isOpen()) {
            logger.warn("ServerSocketChannel close, {}", socketChannel);
            try {
                socketChannel.close();
            } catch (IOException e) {
                logger.warn("ServerSocketChannel close error, {}", socketChannel);
            }
        }
    }

    protected void processAcceptKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        ServerSocketChannel serverSocket = (ServerSocketChannel) curKey.channel();
        // 接收通道，设置为非阻塞模式
        final SocketChannel socketChannel = serverSocket.accept();
        socketChannel.configureBlocking(false);
        logger.info("new Client connected: " + socketChannel);
        ServerType serverType = (ServerType) curKey.attachment();
        ProxyRuntime proxyRuntime = ProxyRuntime.INSTANCE;
        if (!socketChannel.isConnected()){
            throw new RuntimeException("socketChannel is not connected"+socketChannel);
        }
        // 获取附着的标识，即得到当前是否为集群通信端口
        if (serverType == ServerType.CLUSTER) {
            adminSessionMan.createSession(null, this.bufPool, selector, socketChannel, (session, sender, success, result) -> {
                // 客户端连接上来，所以发送信息给客户端
                NodeRegInfoPacket nodeRegInf = new NodeRegInfoPacket(session.cluster().getMyNodeId(),
                        session.cluster().getClusterState(), session.cluster().getLastClusterStateTime(),
                        session.cluster().getMyLeaderId(), ProxyRuntime.INSTANCE.getStartTime());
                session.answerClientNow(nodeRegInf);
            });
        } else if (serverType == ServerType.LOAD_BALANCER && proxyRuntime.getMyCLuster() != null
                && proxyRuntime.getMyCLuster().getClusterState() == ClusterState.Clustered) {
            BalancerConfig balancerConfig = proxyRuntime.getConfig().getConfig(ConfigEnum.BALANCER);
            LoadBalanceStrategy loadBalanceStrategy = LBStrategyConfig
                    .getStrategy(balancerConfig.getBalancer().getStrategy());
            ClusterNode theNode = loadBalanceStrategy.getNode(proxyRuntime.getMyCLuster().allNodes.values(), null);
            String myId = proxyRuntime.getMyCLuster().getMyNodeId();
            if (theNode.id.equals(myId)) {
                logger.debug("load balancer accepted. Dispatch to local");
                accept(reactorEnv, socketChannel, serverType);
            } else {
                logger.debug("load balancer accepted. Dispatch to remote");
                ProxyReactorThread<?> proxyReactor = getLBProxyReactor();
                proxyReactor.addNIOJob(() -> {
                    try {
                        ProxyRuntime.INSTANCE.getLbSessionSessionManager().createSession(null,
                                proxyReactor.bufPool, proxyReactor.getSelector(), socketChannel, (lbSession, sender, success, result) -> {
                                    lbSession.getCurNIOHandler().onConnect(curKey, lbSession, true, null);
                                });
                    } catch (IOException e) {
                        logger.warn("load balancer accepted error:", e);
                    }
                });
            }
        } else {
            // Mycat fontchannel connect
            accept(reactorEnv, socketChannel, serverType);
        }
    }

    private void accept(ReactorEnv reactorEnv, SocketChannel socketChannel, ServerType serverType) throws IOException {
        // 找到一个可用的NIO Reactor Thread，交付托管
        MycatReactorThread[] reactorThreads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
        MycatReactorThread nioReactor = reactorThreads[ThreadLocalRandom.current().nextInt(reactorThreads.length)];
        // 将通道注册到reactor对象上
        nioReactor.acceptNewSocketChannel(serverType, socketChannel);
    }

    private ProxyReactorThread<LBSession> getLBProxyReactor() {
        ProxyReactorThread<LBSession>[] lbThreads = ProxyRuntime.INSTANCE.getLbReactorThreads();
        return lbThreads[ThreadLocalRandom.current().nextInt(lbThreads.length)];
    }

    @SuppressWarnings("unchecked")
    protected void processConnectKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
        // only from cluster server socket
        SocketChannel curChannel = (SocketChannel) curKey.channel();
        Object obj = curKey.attachment();
        try {
            if (curChannel.finishConnect()) {
                if (obj != null && obj instanceof LBSession) {
                    // 负载均衡器连接远程mycat
                    ProxyReactorThread<LBSession> proxyReactor = getLBProxyReactor();
                    proxyReactor.addNIOJob(() -> {
                        try {
                            ProxyRuntime.INSTANCE.getProxySessionSessionManager()
                                    .createSession(null, proxyReactor.bufPool, proxyReactor.getSelector(), curChannel, (proxySession, sender, success, result) -> {
                                        LBSession lbSession = (LBSession) obj;
                                        proxySession.setLbSession(lbSession);
                                        lbSession.setProxySession(proxySession);
                                        proxySession.getCurNIOHandler().onConnect(curKey, proxySession, true, null);
                                    });
                        } catch (Exception e) {
                            logger.warn("Load balancer connect remote mycat error:", e);
                        }
                    });
                } else {
                    adminSessionMan.createSession(curKey.attachment(), this.bufPool, selector,
                            curChannel, (session, sender, success, result) -> {
                                NIOHandler<AdminSession> connectIOHandler = (NIOHandler<AdminSession>) session.getCurNIOHandler();
                                connectIOHandler.onConnect(curKey, session, true, null);
                            });
                }
            }

        } catch (ConnectException ex) {
            logger.warn("connect failed " + curChannel + " reason:" + ex);
            if (adminSessionMan.getDefaultSessionHandler() instanceof NIOHandler) {
                NIOHandler<AdminSession> connectIOHandler = (NIOHandler<AdminSession>) adminSessionMan
                        .getDefaultSessionHandler();
                connectIOHandler.onConnect(curKey, null, false, null);

            }
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

    private void openServerChannel(Selector selector, String bindIp, int bindPort, ServerType serverType)
            throws IOException {
        final ServerSocketChannel serverChannel = ServerSocketChannel.open();
        final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
        serverChannel.bind(isa);
        serverChannel.configureBlocking(false);
        serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        serverChannel.register(selector, SelectionKey.OP_ACCEPT, serverType);
        if (serverType == ServerType.CLUSTER) {
            logger.info("open cluster server port on {}:{}", bindIp, bindPort);
            clusterServerSocketChannel = serverChannel;
        } else if (serverType == ServerType.LOAD_BALANCER) {
            logger.info("open load balance server port on {}:{}", bindIp, bindPort);
            loadBalanceServerSocketChannel = serverChannel;
        } else {
            logger.info("open proxy server port on {}:{}", bindIp, bindPort);
            proxyServerSocketChannel = serverChannel;
        }
    }

    public Selector getSelector() {
        return this.selector;
    }

    public enum ServerType {
        CLUSTER, LOAD_BALANCER, MYCAT;
    }
}
