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
public class Session {
	protected static Logger logger = LoggerFactory.getLogger(Session.class);
	public int sessionId;
	public BufferPool bufPool;
	public Selector nioSelector;
	// 前端连接
	public String frontAddr;
	public SocketChannel frontChannel;
	public SelectionKey frontKey;
	// 保存需要发送到前端连接的数据，即相当于前端连接的writing queue
	// 参考writeToChannel() 与readFromChannel()方法
	public ProxyBuffer frontBuffer;
	// 保存前端收到的数据，即相当于前端的ReadingBuffer，channel.read(backendBuffer)
	public ProxyBuffer backendBuffer;
	// Session是否关闭
	private boolean closed;

	// 当前NIO ProxyHandler
	@SuppressWarnings("rawtypes")
	public NIOHandler curProxyHandler;
	/**
	 * Session会话属性，不能放置大量对象与数据
	 */
	private final Map<String, Object> sessionAttrMap = new HashMap<String, Object>();

	public Session(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this.bufPool = bufferPool;
		this.nioSelector = selector;
		this.frontChannel = channel;
		InetSocketAddress clientAddr = (InetSocketAddress) frontChannel.getRemoteAddress();
		this.frontAddr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		SelectionKey socketKey = frontChannel.register(nioSelector, SelectionKey.OP_READ, this);
		this.frontKey = socketKey;
		frontBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
		backendBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
		this.sessionId = ProxyRuntime.INSTANCE.genSessionId();
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
			bufPool.recycleBuf(backendBuffer.getBuffer());
		} else {
			logger.warn("session already closed " + this.sessionInfo());
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = ((normal) ? " normal close " : "abnormal close  socket");
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
		((FrontIOHandler) curProxyHandler).onFrontSocketClosed(this, normal);

	}

	/**
	 * 向前端发送数据报文，数据报文从0的位置开始写入，覆盖之前任何从后端读来的数据！！
	 * 
	 * @param rawPkg
	 * @throws IOException
	 */
	public void answerFront(byte[] rawPkg) throws IOException {
		frontBuffer.writeBytes(rawPkg);
		frontBuffer.flip();
		writeToChannel(frontBuffer, frontChannel);
	}

	/**
	 * 从SocketChannel中读取数据并写入到内部Buffer中,writeState里记录了写入的位置指针
	 * 第一次调用之前需要确保Buffer状态为Write状态，并指定要写入的位置，
	 * 
	 * @param channel
	 * @return 读取了多少数据
	 */
	public int readFromChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {
		ByteBuffer buffer = proxyBuf.getBuffer();
		buffer.limit(proxyBuf.writeState.optLimit);
		buffer.position(proxyBuf.writeState.optPostion);
		int readed = channel.read(buffer);
		proxyBuf.writeState.curOptedLength = readed;
		if (readed > 0) {
			proxyBuf.writeState.optPostion += readed;
			proxyBuf.writeState.optedTotalLength += readed;
		}
		return readed;
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
				// 切换到写模式，继续从对端Socket读数据
				proxyBuf.setInReading(false);
			} else {
				// 数据彻底写完，切换为读模式
				proxyBuf.flip();
			}

			modifySelectKey();
		} else {
			if (!proxyBuf.isInReading()) {
				proxyBuf.setInReading(true);
				modifySelectKey();
			}
		}
	}

	public void modifySelectKey() throws ClosedChannelException {

		if (frontKey != null && frontKey.isValid()) {
			int clientOps = 0;
			if (backendBuffer.isInWriting())
				clientOps |= SelectionKey.OP_READ;
			if (frontBuffer.isInWriting() == false)
				clientOps |= SelectionKey.OP_WRITE;
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

	@SuppressWarnings("rawtypes")
	public void setCurProxyHandler(NIOHandler proxyHandler) {
		curProxyHandler = proxyHandler;
	}

	@SuppressWarnings("rawtypes")
	public NIOHandler getCurProxyHandler() {
		return curProxyHandler;
	}
}
