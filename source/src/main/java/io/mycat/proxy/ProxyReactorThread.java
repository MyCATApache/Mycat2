package io.mycat.proxy;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
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
public class ProxyReactorThread<T extends Session> extends Thread {
	protected final static long SELECTOR_TIMEOUT = 100;
	protected final SessionManager<T> sessionMan;
	protected final static Logger logger = LoggerFactory.getLogger(ProxyReactorThread.class);
	protected final Selector selector;
	protected final BufferPool bufPool;
	protected ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<Runnable>();
	protected ArrayList<T> allSessions = new ArrayList<T>();

	@SuppressWarnings("unchecked")
	public ProxyReactorThread(BufferPool bufPool) throws IOException {
		this.bufPool = bufPool;
		this.selector = Selector.open();
		sessionMan = (SessionManager<T>) ProxyRuntime.INSTANCE.getSessionManager();
	}

	public void acceptNewSocketChannel(Object keyAttachement, final SocketChannel socketChannel) throws IOException {
		pendingJobs.offer(() -> {
			try {
				T session = sessionMan.createSession(keyAttachement, this.bufPool, selector, socketChannel, true);
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
				logger.warn("run nio job err " , e);
			}
		}

	}

	protected void processAcceptKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {

	}

	@SuppressWarnings("unchecked")
	protected void processConnectKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		try {
			if (((SocketChannel) curKey.channel()).finishConnect()) {
				((BackendIOHandler<T>) session.getCurNIOHandler()).onBackendConnect(session, true, null);
			}

		} catch (ConnectException ex) {
			((BackendIOHandler<T>) session.getCurNIOHandler()).onBackendConnect(session, false, ex.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	protected void processReadKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		// only from cluster server socket
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		if (session.frontChannel() == curKey.channel()) {
			((FrontIOHandler<T>) session.getCurNIOHandler()).onFrontRead(session);
		} else {
			((BackendIOHandler<T>) session.getCurNIOHandler()).onBackendRead(session);
		}
	}

	@SuppressWarnings("unchecked")
	protected void processWriteKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		// only from cluster server socket
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		if (session.frontChannel() == curKey.channel()) {
			((FrontIOHandler<T>) session.getCurNIOHandler()).onFrontWrite(session);
		} else {
			((BackendIOHandler<T>) session.getCurNIOHandler()).onBackendWrite(session);
		}
	}

	public void run() {
		long ioTimes = 0;
		ReactorEnv reactorEnv = new ReactorEnv();
		while (true) {
			try {
				selector.select(SELECTOR_TIMEOUT);
				final Set<SelectionKey> keys = selector.selectedKeys();
				// logger.info("handler keys ,total " + selected);
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
				for (final SelectionKey key : keys) {
					try {
						int readdyOps = key.readyOps();
						reactorEnv.curSession = null;
						// 如果当前收到连接请求
						if ((readdyOps & SelectionKey.OP_ACCEPT) != 0) {
							processAcceptKey(reactorEnv, key);
						}
						// 如果当前连接事件
						else if ((readdyOps & SelectionKey.OP_CONNECT) != 0) {
							this.processConnectKey(reactorEnv, key);
						} else if ((readdyOps & SelectionKey.OP_READ) != 0) {
							this.processReadKey(reactorEnv, key);

						} else if ((readdyOps & SelectionKey.OP_WRITE) != 0) {
							this.processWriteKey(reactorEnv, key);
						}
					} catch (Exception e) {
						logger.warn("Socket IO err :", e);
						key.cancel();
						if (reactorEnv.curSession != null) {
							reactorEnv.curSession.close(false,"Socket IO err:" + e);
							this.allSessions.remove(reactorEnv.curSession);
							reactorEnv.curSession = null;
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
