package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.CurSQLState;
import io.mycat.mycat2.MycatSession;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.util.StringUtil;

/**
 * 会话，代表一个前端连接
 *
 * @author wuzhihui
 */
public abstract class AbstractSession implements Session {

	protected static Logger logger = LoggerFactory.getLogger(AbstractSession.class);

	// 当前SQL上下文状态数据对象
	public final CurSQLState curSQLSate = new CurSQLState();
	public ProxyBuffer proxyBuffer;
	public BufferPool bufPool;
	public Selector nioSelector;
	// 操作的Socket连接
	public String addr;
	public String host;
	public long startTime;
	public SocketChannel channel;
	public SelectionKey channelKey;
	/**
	 * 是否多个Session共用同一个Buffer
	 */
	protected boolean referedBuffer;
	protected boolean defaultChannelRead = true;
	/**
	 * 是否多个Session共用同一个Buffer时，当前Session是否暂时获取了Buffer独家使用权，即独占Buffer
	 */
	protected boolean curBufOwner = true;
	private SessionManager<? extends Session> sessionManager;
	private NIOHandler nioHandler;
	private int sessionId;
	// Session是否关闭
	private boolean closed;

	public AbstractSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this(bufferPool, selector, channel, SelectionKey.OP_READ);
	}

	public AbstractSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int socketOpt)
			throws IOException {
		this.bufPool = bufferPool;
		this.nioSelector = selector;
		this.channel = channel;
		InetSocketAddress clientAddr = (InetSocketAddress) channel.getRemoteAddress();
		this.addr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		this.host = clientAddr.getHostString();
		SelectionKey socketKey = channel.register(nioSelector, socketOpt, this);
		this.channelKey = socketKey;
		this.proxyBuffer = new ProxyBuffer(this.bufPool.allocate());
		this.sessionId = ProxyRuntime.INSTANCE.genSessionId();
		this.startTime = System.currentTimeMillis();
	}

	public AbstractSession() {

	}

	/**
	 * 使用共享的Buffer
	 *
	 * @param sharedBuffer
	 */
	public void useSharedBuffer(ProxyBuffer sharedBuffer) {
		if (this.proxyBuffer != null && referedBuffer == false) {
			recycleAllocedBuffer(proxyBuffer);
			proxyBuffer = sharedBuffer;
			this.referedBuffer = true;
			logger.debug("use sharedBuffer. ");
		} else if (proxyBuffer == null) {
			logger.debug("proxyBuffer is null.{}", this);
			throw new RuntimeException("proxyBuffer is null.");
			// proxyBuffer = sharedBuffer;
		} else if (sharedBuffer == null) {
			logger.debug("referedBuffer is false.");
			proxyBuffer = new ProxyBuffer(this.bufPool.allocate());
			proxyBuffer.reset();
			this.referedBuffer = false;
		}
	}

	public boolean isCurBufOwner() {
		return curBufOwner;
	}

	public ProxyBuffer getProxyBuffer() {
		return proxyBuffer;
	}

	/**
	 * 从SocketChannel中读取数据并写入到内部Buffer中,writeState里记录了写入的位置指针
	 * 第一次调用之前需要确保Buffer状态为Write状态，并指定要写入的位置，
	 *
	 * @return 读取了多少数据
	 */
	public boolean readFromChannel() throws IOException {
		if (!this.proxyBuffer.isInWriting()) {
			throw new java.lang.IllegalArgumentException("buffer not in writing state ");
		} else if (this.curBufOwner == false) {//
			logger.info("take owner for some read data coming ..." + this.sessionInfo());
			doTakeReadOwner();
		}

		ByteBuffer buffer = proxyBuffer.getBuffer();
		if (proxyBuffer.writeIndex > buffer.capacity() * 1 / 3) {
			proxyBuffer.compact();
		} else {
			// buffer.position 在有半包没有参与透传时,会小于 writeIndex。
			// 大部分情况下 position == writeIndex
			buffer.position(proxyBuffer.writeIndex);
		}
		int position = buffer.position();
		int readed = channel.read(buffer);

		try {
			if (readed > 0) {
				final String hexs = StringUtil.dumpAsHex(buffer.duplicate(), position, readed);
				System.out.println(this);
				System.out.println(hexs);

			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		// logger.debug(" readed {} total bytes curChannel is {}", readed,this);
		if (readed == -1) {
			logger.warn("Read EOF ,socket closed ");
			throw new ClosedChannelException();
		} else if (readed == 0) {
			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		proxyBuffer.writeIndex = buffer.position();
		return readed > 0;
	}

	protected abstract void doTakeReadOwner();

	protected void checkBufferOwner(boolean bufferReadstate) {
		if (!curBufOwner) {
			throw new java.lang.IllegalArgumentException("buffer not changed to me ");
		} else if (this.proxyBuffer.isInReading() != bufferReadstate) {
			throw new java.lang.IllegalArgumentException(
					"buffer not in correcte state ,expected state  " + (bufferReadstate ? " readable " : "writable "));
		}
	}

	/**
	 * 从内部Buffer数据写入到SocketChannel中发送出去，readState里记录了写到Socket中的数据指针位置 方法，
	 */
	public void writeToChannel() throws IOException {
		checkBufferOwner(true);
		ByteBuffer buffer = proxyBuffer.getBuffer();
		buffer.limit(proxyBuffer.readIndex);
		buffer.position(proxyBuffer.readMark);
		int writed = channel.write(buffer);
		proxyBuffer.readMark += writed; // 记录本次磁轭如到 Channel 中的数据
		if (!buffer.hasRemaining()) {
			// logger.debug("writeToChannel write {} bytes ,curChannel is {}",
			// writed,this);
			// buffer 中需要透传的数据全部写入到 channel中后,会进入到当前分支.这时 readIndex == readLimit
			if (proxyBuffer.readMark != proxyBuffer.readIndex) {
				logger.error("writeToChannel has finished but readIndex != readLimit, please fix it !!!");
			}
			if (proxyBuffer.readIndex > buffer.capacity() * 2 / 3) {
				proxyBuffer.compact();
			} else {
				buffer.limit(buffer.capacity());
			}
			// 切换读写状态
			// proxyBuffer.flip();
			/*
			 * 如果需要自动切换owner,进行切换 1. writed==0 或者 buffer 中数据没有写完时,注册可写事件
			 * 时,会进行owner 切换 注册写事件,完成后,需要自动切换回来
			 */
			// if (proxyBuf.needAutoChangeOwner()) {
			// proxyBuf.changeOwner(!proxyBuf.frontUsing()).setPreUsing(null);
			// }
		} else {
			/**
			 * 1. writed==0 或者 buffer 中数据没有写完时,注册可写事件 通常发生在网络阻塞或者 客户端
			 * COM_STMT_FETCH 命令可能会 出现没有写完或者 writed == 0 的情况
			 */
			logger.debug("register OP_WRITE  selectkey .write  {} bytes. current channel is {}", writed, channel);
			// 需要切换 owner ,同时保存当前 owner 用于数据传输完成后,再切换回来
			// proxyBuf 读写状态不切换,会切换到相同的事件,不会重复注册
			// proxyBuf.setPreUsing(proxyBuf.frontUsing()).changeOwner(!proxyBuf.frontUsing());
		}
		checkWriteFinished();
	}

	/**
	 * 手动创建的ProxyBuffer需要手动释放，recycleAllocedBuffer()
	 *
	 * @return ProxyBuffer
	 */
	public ProxyBuffer allocNewProxyBuffer() {
		return new ProxyBuffer(bufPool.allocate());
	}

	/**
	 * 释放手动分配的ProxyBuffer
	 *
	 * @param curFrontBuffer
	 */
	public void recycleAllocedBuffer(ProxyBuffer curFrontBuffer) {
		if (curFrontBuffer != null) {
			this.bufPool.recycle(curFrontBuffer.getBuffer());
		} else {
			logger.error("curFrontBuffer is null,please fix it !!!!");
		}
	}

	protected void checkWriteFinished() throws IOException {
		checkBufferOwner(true);
		if (!this.proxyBuffer.writeFinished()) {
			this.change2WriteOpts();
		} else {
			writeFinished();
			// clearReadWriteOpts();
		}
	}

	public void change2ReadOpts() {
		// 不做检查，因为两个chanel不确定哪个会对读事件感兴趣，因此通常会都设置为读感兴趣
		//int intesOpts = this.channelKey.interestOps();
		// 事件转换时,只注册一个事件,存在可写事件没有取消注册的情况。这里把判断取消
		// if ((intesOpts & SelectionKey.OP_READ) != SelectionKey.OP_READ) {
		channelKey.interestOps(SelectionKey.OP_READ);
		// }
	}

	public void clearReadWriteOpts() {
		this.channelKey.interestOps(0);
	}

	public void change2WriteOpts() {
		checkBufferOwner(true);
		//int intesOpts = this.channelKey.interestOps();
		// 事件转换时,只注册一个事件,存在可读事件没有取消注册的情况。这里把判断取消
		// if ((intesOpts & SelectionKey.OP_WRITE) != SelectionKey.OP_WRITE) {
		channelKey.interestOps(SelectionKey.OP_WRITE);
		// }
	}

	@Override
	public SocketChannel channel() {
		return this.channel;
	}

	public String sessionInfo() {
		return " [ sessionId = " + sessionId + " ," + this.addr + ']';
	}

	public boolean isChannelOpen() {
		return channel != null && channel.isConnected();
	}

	public boolean isClosed() {
		return closed;
	}

	/**
	 * 关闭会话（同时关闭连接）
	 *
	 * @param normal
	 */
	public void close(boolean normal, String hint) {
		if (!this.isClosed()) {
			this.closed = true;
			closeSocket(channel, normal, hint);
			if (!referedBuffer) {
				recycleAllocedBuffer(proxyBuffer);
			}
			if (this instanceof MycatSession) {
				this.getMySessionManager().removeSession(this);
			}
		} else {
			logger.warn("session already closed " + this.sessionInfo());
		}

	}

	@SuppressWarnings("rawtypes")
	public void lazyCloseSession(final boolean normal, final String reason) {
		if (isClosed()) {
			return;
		}

		ProxyRuntime.INSTANCE.addDelayedNIOJob(() -> {
			if (!isClosed()) {
				close(normal, reason);
			}
		}, 10, (ProxyReactorThread) Thread.currentThread());
	}

	protected void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = (normal) ? " normal close " : "abnormal close ";
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
	}

	public void addSessionAttr(String attrName, Object value) {
		logger.info("add session attr:" + attrName + " value:" + value);
	}

	public int getSessionId() {
		return sessionId;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Session> SessionManager<T> getMySessionManager() {
		return (SessionManager<T>) this.sessionManager;
	}

	public void setSessionManager(SessionManager<? extends Session> curSessionMan) {
		this.sessionManager = curSessionMan;
	}

	@Override
	public NIOHandler getCurNIOHandler() {
		return nioHandler;
	}

	public void setCurNIOHandler(NIOHandler curNioHandler) {
		this.nioHandler = curNioHandler;
	}

	public void writeFinished() throws IOException {
		this.getCurNIOHandler().onWriteFinished(this);

	}

	public boolean isDefaultChannelRead() {
		return defaultChannelRead;
	}

	public void setDefaultChannelRead(boolean defaultChannelRead) {
		this.defaultChannelRead = defaultChannelRead;
	}

	public boolean isReferedBuffer() {
		return referedBuffer;
	}
}
