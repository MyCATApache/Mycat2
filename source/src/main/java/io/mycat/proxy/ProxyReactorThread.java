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
	private final BufferPool bufPool;
	private ConcurrentLinkedQueue<Runnable> pendingJobs = new ConcurrentLinkedQueue<Runnable>();
	private ArrayList<T> allSessions = new ArrayList<T>();

	@SuppressWarnings("unchecked")
	public ProxyReactorThread(BufferPool bufPool) throws IOException {
		this.bufPool = bufPool;
		this.selector = Selector.open();
		sessionMan = ProxyRuntime.INSTANCE.getSessionManager();
	}

	/**
	 * 进行新的socket通道注册
	 * @param socketChannel 通道信息
	 * @throws IOException
	 */
	public void acceptNewSocketChannel(final SocketChannel socketChannel) throws IOException {
		//将当前通道放入到注册的队列中
		pendingJobs.offer(() -> {
			try {
				//创建会话对象信息,一般指定为MycatSessionManager
				T session = sessionMan.createSession(this.bufPool, selector, socketChannel, true);
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

	/**
	 * 进行事件处理
	 * @param curChannel 当前通道信息
	 * @param session 当前会话信息
	 * @throws IOException 异常信息
	 */
	@SuppressWarnings({ "unchecked" })
	private void handleREvent(final SocketChannel curChannel, final T session) throws IOException {
		//进行前端通道的处理读取事件处理
		if (session.frontChannel == curChannel) {
			((FrontIOHandler<T>) session.curProxyHandler).onFrontRead(session);
		}
		//进行后端通道的读取事件处理
		else {
			((BackendIOHandler<T>) session.curProxyHandler).onBackendRead(session);
		}
	}

	/**
	 * 进行事件的统一调用处理方法
	 * @param curChannel 当前通道信息
	 * @param session 会话对象
	 * @param readdyOps 事件集合
	 * @throws IOException 
	 */
	private void handleWREvent(final SocketChannel curChannel, final T session, int readdyOps) throws IOException {
		//检查当前是否打开了双向通信，如果为mysql则为false，		
		if (ProxyRuntime.isNioBiproxyflag()) {
			if ((readdyOps & SelectionKey.OP_READ) != 0) {
				handleREvent(curChannel, session);
			}
			if ((readdyOps & SelectionKey.OP_WRITE) != 0) {
				handleWEvent(curChannel, session);
			}
		} else {
			//进行读取事件处理
			if ((readdyOps & SelectionKey.OP_READ) != 0) {
				// logger.info("readable keys " + curChannel);
				handleREvent(curChannel, session);
			} 
			//其他均为写入事件处理
			else {
				// logger.info("writable keys " + curChannel);
				handleWEvent(curChannel, session);
			}
		}
	}

	/**
	 * 进行写入事件的处理
	 * @param curChannel 当前通道
	 * @param session session会话对象
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	private void handleWEvent(final SocketChannel curChannel, final T session) throws IOException {
		//前端通道的写入处理
		if (session.frontChannel == curChannel) {
			((FrontIOHandler<T>) session.curProxyHandler).onFrontWrite(session);
		} 
		//后端通道的写入处理
		else {
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
				Iterator<SelectionKey> itor = selector.selectedKeys().iterator();
				while (itor.hasNext()) {
					SelectionKey key = itor.next();
					itor.remove();
					final T session = (T) key.attachment();
					final SocketChannel curChannel = (SocketChannel) key.channel();
					int readdyOps = key.readyOps();
					//处理发送的连接事件，一般为连接后端的事件
					if ((readdyOps & SelectionKey.OP_CONNECT) != 0) {
						logger.info("connectable keys " + key.channel());
						session.backendChannel = curChannel;
						try {
							if (curChannel.finishConnect()) {
								//当连接完成后,调用后端进行处理
								((BackendIOHandler<T>) session.curProxyHandler).onBackendConnect(session, true, null);
							}

						} catch (ConnectException ex) {
							((BackendIOHandler<T>) session.curProxyHandler).onBackendConnect(session, false,
									ex.getMessage());
						}

					} else {
						try {
							//进行统一的事件调用处理
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
