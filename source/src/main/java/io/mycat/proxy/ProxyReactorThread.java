package io.mycat.proxy;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * NIO Reactor Thread 负责多个Session会话
 * 
 * @author wuzhihui
 *
 */
public class ProxyReactorThread extends Thread {
	private final static long SELECTOR_TIMEOUT = 100;
	private final DefaultDirectProxyHandler proxyTransHandler = new DefaultDirectProxyHandler();
	private final static Logger logger = Logger.getLogger(ProxyReactorThread.class);
	private final Selector selector;
	private final BufferPool bufPool = new BufferPool(1024 * 10);
	private ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<Runnable>();

	public ProxyReactorThread() throws IOException {
		this.selector = Selector.open();
	}

	public void acceptNewSocketChannel(final SocketChannel socketChannel) throws IOException {
		pendingJobs.offer(() -> {
			try {

				UserSession session = new UserSession(bufPool, this.selector, socketChannel);
				session.bufPool = bufPool;
				session.nioSelector = selector;
				session.frontChannel = socketChannel;
				InetSocketAddress clientAddr = (InetSocketAddress) socketChannel.getRemoteAddress();
				session.frontAddr=clientAddr.getHostString()+":"+clientAddr.getPort();
				SelectionKey socketKey = socketChannel.register(selector, SelectionKey.OP_READ, session);
				session.frontKey = socketKey;
				proxyTransHandler.onFrontConnected(session);
			} catch (Exception e) {
				logger.warn("regist new connection err " + e);
			}
		});

	}

	public void addNIOJob(Runnable job) {
		pendingJobs.offer(job);
	}

	private void processNIOJob() {
		Runnable nioJob = null;
		while ((nioJob = pendingJobs.poll()) != null) {
			try {
				nioJob.run();
			} catch (Exception e) {
				logger.warn("run nio job err " + e);
			}
		}

	}

	public void run() {
		long ioTimes = 0;
		while (true) {

			try {
				int selected = selector.select(SELECTOR_TIMEOUT);
				if (selected == 0) {
					if (!pendingJobs.isEmpty()) {
						ioTimes = 0;
						this.processNIOJob();
					}
					continue;
				} else if ((ioTimes > 5) & !pendingJobs.isEmpty()) {
					ioTimes = 0;
					this.processNIOJob();
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
						try {
							if (curChannel.isConnectionPending()) {
								curChannel.finishConnect();
							}
							proxyTransHandler.onBackendConnect(session, true, null);
						} catch (ConnectException ex) {
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

	
}
