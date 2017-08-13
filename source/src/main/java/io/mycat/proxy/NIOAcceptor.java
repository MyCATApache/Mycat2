package io.mycat.proxy;

/**
 * NIO Acceptor ,只用来接受新连接的请求，也负责处理管理端口的报文
 * @author wuzhihui
 *
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.man.AdminSession;

public class NIOAcceptor extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(NIOAcceptor.class);
	private BufferPool bufferPool;
	private final Selector selector;

	public NIOAcceptor(BufferPool bufferPool) throws IOException {
		this.setName("NIO-Acceptor");
		this.bufferPool = bufferPool;
		selector = Selector.open();
	}

	@SuppressWarnings("rawtypes")
	public void run() {
		int nioIndex = 0;

		ProxyRuntime env = ProxyRuntime.INSTANCE;
		ProxyConfig conf = env.getProxyConfig();
		try {

			openServerChannel(selector, conf.getBindIP(), conf.getBindPort(), false);
			if (conf.isClusterEnable()) {
				logger.info("opend cluster conmunite port on " + conf.getClusterIP() + ':' + conf.getClusterPort());
				openServerChannel(selector, conf.getClusterIP(), conf.getClusterPort(), true);
			}

		} catch (IOException e) {
			System.out.println(" NIOAcceptor start err " + e);
			return;
		}

		while (true) {
			try {
				selector.select(1000);
				final Set<SelectionKey> keys = selector.selectedKeys();
				if (keys.isEmpty()) {
					continue;
				}

				for (final SelectionKey key : keys) {
					if (!key.isValid()) {
						continue;
					}
					int readdyOps = key.readyOps();
					if ((readdyOps & SelectionKey.OP_ACCEPT) != 0) {

						ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
						final SocketChannel socketChannel = serverSocket.accept();
						socketChannel.configureBlocking(false);
						logger.info("new Client connected: " + socketChannel);
						boolean clusterServer = (boolean) key.attachment();
						if (clusterServer) {
							Session session = env.getAdminSessionManager().createSession(this.bufferPool, selector,
									socketChannel, true);
						} else {
							// 找到一个可用的NIO Reactor Thread，交付托管
							if (nioIndex++ == Integer.MAX_VALUE) {
								nioIndex = 1;
							}
							int index = nioIndex % env.getNioReactorThreads();
							ProxyReactorThread nioReactor = env.getReactorThreads()[index];
							nioReactor.acceptNewSocketChannel(socketChannel);
						}
					} else if ((readdyOps & SelectionKey.OP_CONNECT) != 0) {
						// only from cluster server socket
						SocketChannel socketChannel = (SocketChannel) key.channel();
						if (socketChannel.finishConnect()) {
							Session session = env.getAdminSessionManager().createSession(this.bufferPool, selector,
									socketChannel, false);
						}

					} else if ((readdyOps & SelectionKey.OP_READ) != 0) {
						// only from cluster server socket
						env.getAdminSessionIOHandler().onFrontRead((AdminSession) key.attachment());
					} else if ((readdyOps & SelectionKey.OP_WRITE) != 0) {
						// only from cluster server socket
						env.getAdminSessionIOHandler().onFrontWrite((AdminSession) key.attachment());
					}
				}
				keys.clear();
			} catch (IOException e) {
				logger.warn("caugh error ", e);
			}
		}

	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public void setBufferPool(BufferPool bufferPool) {
		this.bufferPool = bufferPool;
	}

	public Selector getSelector() {
		return selector;
	}

	private void openServerChannel(Selector selector, String bindIp, int bindPort, boolean clusterServer)
			throws IOException, ClosedChannelException {
		final ServerSocketChannel serverChannel = ServerSocketChannel.open();

		final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
		serverChannel.bind(isa);
		serverChannel.configureBlocking(false);
		serverChannel.register(selector, SelectionKey.OP_ACCEPT, clusterServer);
	}

}
