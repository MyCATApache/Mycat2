package io.mycat.proxy;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * NIO Reactor Thread 负责代理通信中的NIO事件派发
 * 
 * @author wuzhihui
 *
 */
public class ProxyReactorThread extends Thread {
	private final static long SELECTOR_TIMEOUT = 100;
	private final ProxyTransDataNIOHandler proxyTransHandler = new ProxyTransDataNIOHandler();
	private final static Logger logger = Logger.getLogger(ProxyReactorThread.class);
	private final Selector selector;
	private final BufferPool bufPool = new BufferPool(1024 * 10);
	private ConcurrentLinkedQueue<SocketChannel> pendingCons = new ConcurrentLinkedQueue<SocketChannel>();

	public ProxyReactorThread() throws IOException {
		this.selector = Selector.open();
	}

	public void acceptNewSocketChannel(final SocketChannel socketChannel) throws IOException {
		pendingCons.offer(socketChannel);

	}

	private void processNewCons() {
		SocketChannel socketChannel = null;
		while ((socketChannel = pendingCons.poll()) != null) {
			try {

				UserSession session = new UserSession(bufPool, this.selector, socketChannel);
				session.bufPool = bufPool;
				session.nioSelector = selector;
				session.frontChannel = socketChannel;
				SelectionKey socketKey = socketChannel.register(selector, SelectionKey.OP_READ, session);
				session.frontKey=socketKey;
				proxyTransHandler.onFrontConnected(session);
			} catch (Exception e) {
				logger.warn("regist new connection err " + e);
			}
		}

	}

	public void run() {
		long ioTimes = 0;
		while (true) {

			try {
				int selected = selector.select(SELECTOR_TIMEOUT);
				if (selected == 0) {
					if (!pendingCons.isEmpty()) {
						ioTimes = 0;
						this.processNewCons();
					}
					continue;
				} else if ((ioTimes > 5) & !pendingCons.isEmpty()) {
					ioTimes = 0;
					this.processNewCons();
				}
				ioTimes++;
				final Set<SelectionKey> keys = selector.selectedKeys();
				for (final SelectionKey key : keys) {
					if (!key.isValid()) {
						continue;
					}
					final UserSession session = (UserSession) key.attachment();
					if (key.isConnectable()) {
						SocketChannel curChannel = (SocketChannel) key.channel();
						session.backendChannel = curChannel;
						try{
						if (curChannel.isConnectionPending()) {
							curChannel.finishConnect();
						}
						proxyTransHandler.onBackendConnect(session, true, null);
						}catch(ConnectException ex)
						{
							proxyTransHandler.onBackendConnect(session, false, ex.getMessage());
						}
						
						
					} else {
						proxyTransHandler.handIO(session, key);
					}
				}
				keys.clear();
			} catch (IOException e) {
				logger.warn("caugh error ", e);
			}
		}
	}

	public static void closeQuietly(final Closeable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (IOException e) {
		}
	}
}
