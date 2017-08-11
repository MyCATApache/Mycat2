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

import org.slf4j.*;

public class NIOAcceptor extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(NIOAcceptor.class);
	private BufferPool bufferPool;

	public NIOAcceptor(BufferPool bufferPool) {
		this.bufferPool = bufferPool;
	}

	@SuppressWarnings("rawtypes")
	public void run() {
		int nioIndex = 0;
		Selector selector = null;
		ProxyRuntime env = ProxyRuntime.INSTANCE;
		ProxyConfig conf = env.getProxyConfig();
		try {
			selector = Selector.open();
			openServerChannel(selector, conf.getBindIP(), conf.getBindPort(), false);
			if (conf.isAdminPortEnable()) {
				logger.info("opend manage port on " + conf.getAdminIP() + ':' + conf.getAdminPort());
				openServerChannel(selector, conf.getAdminIP(), conf.getAdminPort(), false);
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
					if (key.isAcceptable()) {

						ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
						final SocketChannel socketChannel = serverSocket.accept();
						socketChannel.configureBlocking(false);
						logger.info("new Client connected: " + socketChannel);
						boolean adminServer = (boolean) key.attachment();
						if (adminServer) {
							Session session = env.getAdminSessionManager().createSession(this.bufferPool, selector,
									socketChannel);
						} else {
							// 找到一个可用的NIO Reactor Thread，交付托管
							if (nioIndex++ == Integer.MAX_VALUE) {
								nioIndex = 1;
							}
							int index = nioIndex % env.getNioReactorThreads();
							ProxyReactorThread nioReactor = env.getReactorThreads()[index];
							nioReactor.acceptNewSocketChannel(socketChannel);
						}
					} else {
						logger.warn("not accept event " + key);
					}
				}
				keys.clear();
			} catch (IOException e) {
				logger.warn("caugh error ", e);
			}
		}

	}

	private void openServerChannel(Selector selector, String bindIp, int bindPort, boolean adminServer)
			throws IOException, ClosedChannelException {
		final ServerSocketChannel serverChannel = ServerSocketChannel.open();

		final InetSocketAddress isa = new InetSocketAddress(bindIp, bindPort);
		serverChannel.bind(isa);
		serverChannel.configureBlocking(false);
		serverChannel.register(selector, SelectionKey.OP_ACCEPT, adminServer);
	}

}
