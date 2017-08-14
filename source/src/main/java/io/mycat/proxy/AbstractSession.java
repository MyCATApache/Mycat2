package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 会话，代表一个前端连接
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractSession implements Session {

	protected static Logger logger = LoggerFactory.getLogger(AbstractSession.class);
	private SessionManager<? extends Session> sessionManager;
	private NIOHandler<? extends Session> nioHandler;
	private int sessionId;
	public BufferPool bufPool;
	public Selector nioSelector;
	// 前端连接
	public String frontAddr;
	public SocketChannel frontChannel;
	public SelectionKey frontKey;
	// 保存需要发送到前端连接的数据，即相当于前端连接的writing queue
	// 参考writeToChannel() 与readFromChannel()方法
	public ProxyBuffer frontBuffer;

	// Session是否关闭
	private boolean closed;

	/**
	 * Session会话属性，不能放置大量对象与数据
	 */
	private final Map<String, Object> sessionAttrMap = new HashMap<String, Object>();

	public AbstractSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this.bufPool = bufferPool;
		this.nioSelector = selector;
		this.frontChannel = channel;
		InetSocketAddress clientAddr = (InetSocketAddress) frontChannel.getRemoteAddress();
		this.frontAddr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		SelectionKey socketKey = frontChannel.register(nioSelector, SelectionKey.OP_READ, this);
		this.frontKey = socketKey;
		frontBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
		this.sessionId = ProxyRuntime.INSTANCE.genSessionId();
	}

	@Override
	public SocketChannel frontChannel() {
		return this.frontChannel;
	}

	public String sessionInfo() {
		return " [" + this.frontAddr + ']';
	}

	public boolean isFrontOpen() {
		return frontChannel != null && frontChannel.isConnected();
	}

	public boolean isClosed() {
		return closed;
	}

	/**
	 * 关闭会话（同时关闭连接）
	 * 
	 * @param message
	 */
	public void close(String message) {
		if (!this.isClosed()) {
			this.closed = true;
			logger.info("close session " + this.sessionInfo() + " for reason " + message);
			closeSocket(frontChannel);
			bufPool.recycleBuf(frontBuffer.getBuffer());
			this.getMySessionManager().removeSession(this);
		} else {
			logger.warn("session already closed " + this.sessionInfo());
		}

	}

	/**
	 * 从SocketChannel中读取数据并写入到内部Buffer中,writeState里记录了写入的位置指针
	 * 第一次调用之前需要确保Buffer状态为Write状态，并指定要写入的位置，
	 * 
	 * @param channel
	 * @return 读取了多少数据
	 */
	public boolean readFromChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {

		ByteBuffer buffer = proxyBuf.getBuffer();
		buffer.limit(proxyBuf.writeState.optLimit);
		buffer.position(proxyBuf.writeState.optPostion);
		int readed = channel.read(buffer);
		logger.debug(" readed {} total bytes ,channel {}", readed, channel);
		proxyBuf.writeState.curOptedLength = readed;
		if (readed > 0) {
			proxyBuf.writeState.optPostion += readed;
			proxyBuf.writeState.optedTotalLength += readed;
			proxyBuf.readState.optLimit = proxyBuf.writeState.optPostion;
		} else if (readed == -1) {
			logger.warn("Read EOF ,socket closed ");
			throw new ClosedChannelException();
		} else if (readed == 0) {
			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		return readed > 0;
	}

	/**
	 * 从内部Buffer数据写入到SocketChannel中发送出去，readState里记录了写到Socket中的数据指针位置 方法，
	 * 
	 * @param channel
	 */
	public void writeToChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {
		ByteBuffer buffer = proxyBuf.getBuffer();
		BufferOptState readState = proxyBuf.readState;
		BufferOptState writeState = proxyBuf.writeState;
		buffer.position(readState.optPostion);
		buffer.limit(readState.optLimit);
		int writed = channel.write(buffer);
		readState.curOptedLength = writed;
		readState.optPostion += writed;
		readState.optedTotalLength += writed;
		if (buffer.remaining() == 0) {
			if (writeState.optPostion > buffer.position()) {
				// 当前Buffer中写入的数据多于透传出去的数据，因此透传并未完成
				// compact buffer to head
				buffer.limit(writeState.optPostion);
				buffer.compact();
				readState.optPostion = 0;
				readState.optLimit = buffer.position();
				writeState.optPostion = buffer.position();
				// 继续从对端Socket读数据

			} else {
				// 数据彻底写完，切换为读模式，对端读取数据
				proxyBuf.changeOwner(!proxyBuf.frontUsing());
				proxyBuf.flip();
				modifySelectKey();
			}
		}
	}

	public void modifySelectKey() throws ClosedChannelException {
		if (frontKey != null && frontKey.isValid()) {
			int clientOps = SelectionKey.OP_READ;
			if (frontBuffer.isInWriting() == false) {
				clientOps = SelectionKey.OP_WRITE;
			}
			frontKey.interestOps(clientOps);
		}
	}

	protected void closeSocket(Channel channel) {
		if (channel != null && channel.isOpen()) {
			try {
				channel.close();
			} catch (IOException e) {
				//
			}

		}
	}

	/**
	 * 手动创建的ProxyBuffer需要手动释放，recycleAllocedBuffer()
	 * 
	 * @return ProxyBuffer
	 */
	public ProxyBuffer allocNewProxyBuffer() {
		logger.info("alloc new ProxyBuffer ");
		return new ProxyBuffer(bufPool.allocByteBuffer());
	}

	/**
	 * 释放手动分配的ProxyBuffer
	 * 
	 * @param curFrontBuffer
	 */
	public void recycleAllocedBuffer(ProxyBuffer curFrontBuffer) {
		logger.info("recycle alloced ProxyBuffer ");

		if (curFrontBuffer != null) {
			this.bufPool.recycleBuf(curFrontBuffer.getBuffer());
		}
	}

	public void addSessionAttr(String attrName, Object value) {
		logger.info("add session attr:" + attrName + " value:" + value);
	}

	public Map<String, Object> getSessionAttrMap() {
		return sessionAttrMap;
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

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Session> NIOHandler<T> getCurNIOHandler() {
		return (NIOHandler<T>) nioHandler;
	}

	public void setCurNIOHandler(NIOHandler<? extends Session> curNioHandler) {
		this.nioHandler = curNioHandler;
	}

}
