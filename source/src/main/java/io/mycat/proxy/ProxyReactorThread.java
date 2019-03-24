package io.mycat.proxy;

import io.mycat.proxy.buffer.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * NIO Reactor Thread 负责多个Session会话
 * 
 * @author wuzhihui cjw
 * @checked
 */
public class ProxyReactorThread<T extends Session> extends Thread {
	protected final static long SELECTOR_TIMEOUT = 100;
	protected final SessionManager<T> sessionMan;
	protected final static Logger logger = LoggerFactory.getLogger(ProxyReactorThread.class);
	protected final Selector selector;
	protected final BufferPool bufPool;
	//用于管理连接等事件
	protected final ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<>();



	public Selector getSelector() {
		return selector;
	}

	@SuppressWarnings("unchecked")
	public ProxyReactorThread(BufferPool bufPool) throws IOException {
		this.bufPool = bufPool;
		this.selector = Selector.open();
		sessionMan = (SessionManager<T>) ProxyRuntime.INSTANCE.getSessionManager();
	}

	/**
	 * 处理连接请求
	 * @param keyAttachement
	 * @param socketChannel
	 */
	public void acceptNewSocketChannel(Object keyAttachement, final SocketChannel socketChannel) {
		pendingJobs.offer(() -> {
			try {
				sessionMan.createSession(keyAttachement, this.bufPool, selector, socketChannel,null);
			} catch (Exception e) {
				e.printStackTrace();
				logger.warn("register new connection error " + e);
			}
		});
	}

	public BufferPool getBufPool() {
		return bufPool;
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
				logger.warn("run nio job err ", e);
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
				session.getCurNIOHandler().onConnect(curKey, session, true, null);
			}

		} catch (ConnectException ex) {
			session.getCurNIOHandler().onConnect(curKey, session, false, ex.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	protected void processReadKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		session.getCurNIOHandler().onSocketRead(session);
	}

	@SuppressWarnings("unchecked")
	protected void processWriteKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		session.getCurNIOHandler().onSocketWrite(session);
	}

	public void run() {
		long ioTimes = 0;
		ReactorEnv reactorEnv = new ReactorEnv();
		while (true) {
			try {
				selector.select(SELECTOR_TIMEOUT);
				final Set<SelectionKey> keys = selector.selectedKeys();
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
					} catch (IOException e) {//如果设置为IOException方便调试,避免吞没其他类型异常
						if (logger.isWarnEnabled()) {
							logger.warn("Socket IO err :", e);
						}
						key.cancel();
						if (reactorEnv.curSession != null) {
							reactorEnv.curSession.close(false, "Socket IO err:" + e);
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
