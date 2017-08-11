package io.mycat.proxy;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NIO Reactor Thread 负责多个Session会话
 * 
 * @author wuzhihui
 *
 */
public class ProxyReactorThread<T extends UserProxySession> extends Thread {
	private final static long SELECTOR_TIMEOUT = 1000;
	private final SessionManager<T> sessionMan;
	private final static Logger logger = LoggerFactory.getLogger(ProxyReactorThread.class);
	private final Selector selector;
	private final BufferPool bufPool ;
	private ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<Runnable>();
	private ArrayList<T> allSessions = new ArrayList<T>();

	@SuppressWarnings("unchecked")
	public ProxyReactorThread( BufferPool bufPool) throws IOException {
		this.bufPool=bufPool;
		this.selector = Selector.open();
		sessionMan = ProxyRuntime.INSTANCE.getSessionManager();
	}

	public void acceptNewSocketChannel(final SocketChannel socketChannel) throws IOException {
		pendingJobs.offer(() -> {
			try {
				T session = sessionMan.createSession(this.bufPool, selector, socketChannel);
				allSessions.add(session);
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

	@SuppressWarnings({ "unchecked" })
	private void handleREvent(final SocketChannel curChannel, final T session) throws IOException {
		if (session.frontChannel == curChannel) {
			((FrontIOHandler<T>) session.curProxyHandler).onFrontRead(session);
		} else {
			((BackendIOHandler<T>) session.curProxyHandler).onBackendRead(session);
		}
	}

	private void handleWREvent(final SocketChannel curChannel, final T session, int readdyOps) throws IOException {
		if (ProxyRuntime.isNioBiproxyflag()) {
			if ((readdyOps & SelectionKey.OP_READ) != 0) {
				handleREvent(curChannel, session);
			}
			if ((readdyOps & SelectionKey.OP_WRITE) != 0) {
				handleWEvent(curChannel, session);
			}
		} else {
			if ((readdyOps & SelectionKey.OP_READ) != 0) {
				//logger.info("readable keys " + curChannel);
				handleREvent(curChannel, session);
			} else {
				//logger.info("writable keys " + curChannel);
				handleWEvent(curChannel, session);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void handleWEvent(final SocketChannel curChannel, final T session) throws IOException {
		if (session.frontChannel == curChannel) {
			((FrontIOHandler<T>) session.curProxyHandler).onFrontWrite(session);
		} else {
			((BackendIOHandler<T>) session.curProxyHandler).onBackendWrite(session);
		}
	}

	@SuppressWarnings("unchecked")
	public void run() {
		long ioTimes = 0;
		while (true) {
			try {
				selector.select(SELECTOR_TIMEOUT);
				final Set<SelectionKey> keys = selector.selectedKeys();
				//logger.info("handler keys ,total " + selected);
				if (keys.isEmpty()) {
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
				Iterator<SelectionKey> itor = selector.selectedKeys().iterator();
				while (itor.hasNext()) {
					SelectionKey key = itor.next();
					itor.remove();
					final T session = (T) key.attachment();
					final SocketChannel curChannel = (SocketChannel) key.channel();
					int readdyOps = key.readyOps();
					if ((readdyOps & SelectionKey.OP_CONNECT) != 0) {
						logger.info("connectable keys " + key.channel());
						session.backendChannel = curChannel;
						try {
							if (curChannel.finishConnect()) {
								((BackendIOHandler<T>) session.curProxyHandler).onBackendConnect(session, true, null);
							}

						} catch (ConnectException ex) {
							((BackendIOHandler<T>) session.curProxyHandler).onBackendConnect(session, false,
									ex.getMessage());
						}

					} else {
						try {
							handleWREvent(curChannel, session, readdyOps);
						} catch (Exception e) {
							logger.warn("Socket IO err :", e);
							session.close("Socket IO err:" + e);
							this.allSessions.remove(session);
						}
					}

				}
				keys.clear();

			} catch (IOException e) {
				logger.warn("caugh error ", e);
			}

		}

	}

}
