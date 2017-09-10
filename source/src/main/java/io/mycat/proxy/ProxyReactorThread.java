package io.mycat.proxy;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLMetaBean;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mycat2.tasks.AsynTaskCallBack;
import io.mycat.mycat2.tasks.BackendConCreateTask;
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
	protected LinkedList<T> allSessions = new LinkedList<T>();

	// 存放后端连接的map
	protected Map<MySQLMetaBean, LinkedList<MySQLSession>> mySQLSessionMap = new HashMap<>();

	public Selector getSelector() {
		return selector;
	}

	public LinkedList<T> getAllSessions() {
		return allSessions;
	}

	public void createSession(MySQLMetaBean mySQLMetaBean, SchemaBean schema, AsynTaskCallBack<MySQLSession> callBack) throws IOException {
		int count = Stream.of(ProxyRuntime.INSTANCE.getReactorThreads())
						.map(session -> session.mySQLSessionMap.get(mySQLMetaBean))
						.filter(list -> list != null)
						.reduce(0, (sum, list) -> sum += list.size(), (sum1, sum2) -> sum1 + sum2)
				+ getUsingBackendConCounts(mySQLMetaBean);
		if (count + 1 > mySQLMetaBean.getMaxCon()) {
			throw new RuntimeException("connection full for " + mySQLMetaBean.getHostName());
		}

		BackendConCreateTask authProcessor = new BackendConCreateTask(bufPool, selector, mySQLMetaBean, schema);
		authProcessor.setCallback(callBack);
	}

	public void addMySQLSession(MySQLMetaBean mySQLMetaBean, MySQLSession mySQLSession) {
		LinkedList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(mySQLMetaBean);
		if (mySQLSessionList == null) {
			mySQLSessionList = new LinkedList<>();
			mySQLSessionMap.put(mySQLMetaBean, mySQLSessionList);
		}
		mySQLSessionList.add(mySQLSession);
	}

	public MySQLSession getExistsSession(MySQLMetaBean mySQLMetaBean) {
		LinkedList<MySQLSession> mySQLSessionList = mySQLSessionMap.get(mySQLMetaBean);
		if (mySQLSessionList != null && !mySQLSessionList.isEmpty()) {
			return mySQLSessionList.removeLast();
		}
		return null;
	}

	/**
	 * 统计后端正在使用的连接数
     */
	private int getUsingBackendConCounts(MySQLMetaBean mySQLMetaBean) {
		return allSessions.stream()
//				.filter(session -> session instanceof MycatSession)
				.map(session -> {
					MycatSession mycatSession = (MycatSession) session;
					return mycatSession.getBackendConCounts(mySQLMetaBean);
				})
				.reduce(0, (sum, count) -> sum += count, (sum1, sum2) -> sum1 + sum2);
	}

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
		// only from cluster server socket
		T session = (T) curKey.attachment();
		reactorEnv.curSession = session;
		session.getCurNIOHandler().onSocketRead(session);
	}

	@SuppressWarnings("unchecked")
	protected void processWriteKey(ReactorEnv reactorEnv, SelectionKey curKey) throws IOException {
		// only from cluster server socket
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
							reactorEnv.curSession.close(false, "Socket IO err:" + e);
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
