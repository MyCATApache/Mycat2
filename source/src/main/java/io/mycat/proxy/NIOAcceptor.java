package io.mycat.proxy;

/**
 * NIO Acceptor ,只用来接受新连接的请求，并不处理任何连接的业务逻辑
 * @author wuzhihui
 *
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;


import org.slf4j.*;

public class NIOAcceptor extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(NIOAcceptor.class);

	@SuppressWarnings("rawtypes")
	public void run() {
		int nioIndex = 0;
		Selector selector = null;
		ProxyRuntime env=ProxyRuntime.INSTANCE;
		ProxyConfig conf=env.getProxyConfig();
		try {
			selector = Selector.open();
			final ServerSocketChannel serverChannel = ServerSocketChannel.open();
			String bindAddr = conf.getBindIP();
			final InetSocketAddress isa = new InetSocketAddress(bindAddr, conf.getBindPort());
			serverChannel.bind(isa);
			serverChannel.configureBlocking(false);
			serverChannel.register(selector, SelectionKey.OP_ACCEPT);

		} catch (IOException e) {
			System.out.println(" NIOAcceptor start err " + e);
			return;
		}
		logger.info("*** Mycat NIO Proxy Server  *** ,NIO Threads " + env.getNioReactorThreads()
				+ " listen on " + conf.getBindIP() + ":" + conf.getBindPort());
		while (true) {
			try {
				int count = selector.select(1000);
				if (count == 0) {
					continue;
				}
				final Set<SelectionKey> keys = selector.selectedKeys();
				for (final SelectionKey key : keys) {
					if (!key.isValid()) {
						continue;
					}
					if (key.isAcceptable()) {
						ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
						final SocketChannel socketChannel = serverSocket.accept();
						socketChannel.configureBlocking(false);
						logger.info("new Client connected: " + socketChannel);
						// 找到一个可用的NIO Reactor Thread，交付托管
						if (nioIndex++ == Integer.MAX_VALUE) {
							nioIndex = 1;
						}
						int index = nioIndex%env.getNioReactorThreads();
						ProxyReactorThread nioReactor =env.getReactorThreads()[index];
						nioReactor.acceptNewSocketChannel(socketChannel);
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

}
