package io.mycat.proxy;

/**
 * NIO Acceptor ,只用来接受新连接的请求，也负责处理管理端口的报文
 * @author wuzhihui
 *
 */
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.man.AdminSession;
import io.mycat.proxy.man.DefaultAdminSessionManager;

public class NIOAcceptor extends ProxyReactorThread<Session> {
	private final static Logger logger = LoggerFactory.getLogger(NIOAcceptor.class);
	protected SessionManager<AdminSession> adminSessionMan;

	public NIOAcceptor(BufferPool bufferPool) throws IOException {
		super(bufferPool);
		this.setName("NIO-Acceptor");

		ProxyRuntime env = ProxyRuntime.INSTANCE;
		ProxyConfig conf = env.getProxyConfig();
		// 打开服务端的socket，普通单节点的mycat，集群标识为false
		openServerChannel(selector, conf.getBindIP(), conf.getBindPort(), false);
		// 检查集群标识是否打开
		if (conf.isClusterEnable()) {
			adminSessionMan = new DefaultAdminSessionManager();
			env.setAdminSessionManager(adminSessionMan);
			logger.info("opend cluster conmunite port on " + conf.getClusterIP() + ':' + conf.getClusterPort());
			// 打开集群通信的socket
			openServerChannel(selector, conf.getClusterIP(), conf.getClusterPort(), true);
		}
	}

	protected void processAcceptKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		ServerSocketChannel serverSocket = (ServerSocketChannel) curKey.channel();
		// 接收通道，设置为非阻塞模式
		final SocketChannel socketChannel = serverSocket.accept();
		socketChannel.configureBlocking(false);
		logger.info("new Client connected: " + socketChannel);
		boolean clusterServer = (boolean) curKey.attachment();
		// 获取附着的标识，即得到当前是否为集群通信端口
		if (clusterServer) {
			adminSessionMan.createSession(null, this.bufPool, selector, socketChannel, true);
		} else {
			// 找到一个可用的NIO Reactor Thread，交付托管
			if (reactorEnv.counter++ == Integer.MAX_VALUE) {
				reactorEnv.counter = 1;
			}
			int index = reactorEnv.counter % ProxyRuntime.INSTANCE.getNioReactorThreads();
			// 获取一个reactor对象
			ProxyReactorThread<?> nioReactor = ProxyRuntime.INSTANCE.getReactorThreads()[index];
			// 将通道注册到reactor对象上
			nioReactor.acceptNewSocketChannel(clusterServer, socketChannel);
		}
	}

	@SuppressWarnings("unchecked")
	protected void processConnectKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		// only from cluster server socket
		SocketChannel curChannel = (SocketChannel) curKey.channel();
		try {
			if (curChannel.finishConnect()) {
				AdminSession session = adminSessionMan.createSession(curKey.attachment(), this.bufPool, selector,
						curChannel, false);
				ConnectIOHandler<AdminSession> connectIOHandler = (ConnectIOHandler<AdminSession>) session
						.getCurNIOHandler();
				connectIOHandler.onConnect(curKey, session, true, null);
			}

		} catch (ConnectException ex) {
			logger.warn("connect failed " + curChannel + " reason:" + ex);
			if (adminSessionMan.getDefaultSessionHandler() instanceof ConnectIOHandler) {
				ConnectIOHandler<AdminSession> connectIOHandler = (ConnectIOHandler<AdminSession>) adminSessionMan
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

	@SuppressWarnings("unchecked")
	protected void processWriteKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		// only from cluster server socket
		Session session = (Session) curKey.attachment();
		session.getCurNIOHandler().onSocketWrite(session);
	}

	private void openServerChannel(Selector selector, String bindIp, int bindPort, boolean clusterServer)
			throws IOException, ClosedChannelException {
		final ServerSocketChannel serverChannel = ServerSocketChannel.open();

		final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
		serverChannel.bind(isa);
		serverChannel.configureBlocking(false);
		serverChannel.register(selector, SelectionKey.OP_ACCEPT, clusterServer);
	}

	public Selector getSelector() {
		return this.selector;
	}

}
