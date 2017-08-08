package io.mycat.proxy;

import java.io.IOException;
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
 * 代表用户的会话，存放用户会话数据，如前端连接，后端连接，状态等数据
 * 
 * @author wuzhihui
 *
 */
public class UserSession {
	public enum NetOptMode {
		// 直接透传此报文到对端，只处理前端读写，只处理后端读写
		DirectTrans, FrontRW, BackendRW
	}

	/**
	 * 默认是透传报文模式，联动两边的NIO事件
	 */
	public NetOptMode netOptMode = NetOptMode.DirectTrans;
	protected static Logger logger = LoggerFactory.getLogger(UserSession.class);
	public int sessionId;
	public BufferPool bufPool;
	public Selector nioSelector;
	// 前端连接
	public String frontAddr;
	public SocketChannel frontChannel;
	public SelectionKey frontKey;
	// 保存需要发送到前端连接的数据，透传模式下，通常一端的Socket会把读到的数据写入到对端的Buffer里
	// 参考writeToChannel() 与readFromChannel()方法
	public ProxyBuffer frontBuffer;
	// 后端连接
	public String backendAddr;
	public SocketChannel backendChannel;
	public SelectionKey backendKey;
	public ProxyBuffer backendBuffer;
	private boolean closed;
	// 当前NIO ProxyHandler
	public NIOHandler curProxyHandler;
	/**
	 * Session会话属性，不能放置大量对象与数据
	 */
	private final Map<String, Object> sessionAttrMap = new HashMap<String, Object>();

	public UserSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) {
		this.bufPool = bufPool;
		this.nioSelector = nioSelector;
		this.frontChannel = frontChannel;
		frontBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
		backendBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
		this.sessionId = ProxyRuntime.INSTANCE.genSessionId();
	}

	public ProxyBuffer allocNewProxyBuffer() {
		logger.info("alloc new ProxyBuffer ");
		return new ProxyBuffer(bufPool.allocByteBuffer());
	}

	public boolean isFrontOpen() {
		return frontChannel != null && frontChannel.isConnected();
	}

	public boolean isBackendOpen() {
		return backendChannel != null && backendChannel.isConnected();
	}

	public String sessionInfo() {
		return " [" + this.frontAddr + "->" + this.backendAddr + ']';
	}

	public boolean isClosed() {
		return closed;
	}

	@SuppressWarnings("rawtypes")
	public void lazyCloseSession() {
		if (isClosed()) {
			return;
		}

		ProxyRuntime.INSTANCE.addDelayedNIOJob(() -> {
			if (!isClosed()) {
				close("front closed");
			}
		}, 10, (ProxyReactorThread) Thread.currentThread());
	}

	public void close(String message) {
		if (!this.isClosed()) {
			logger.info("close session " + this.sessionInfo() + " for reason " + message);
			closeSocket(frontChannel);
			bufPool.recycleBuf(frontBuffer.getBuffer());
			closeSocket(backendChannel);
			bufPool.recycleBuf(this.backendBuffer.getBuffer());
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
	public int readFromChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {
		ByteBuffer buffer = proxyBuf.getBuffer();
		buffer.position(proxyBuf.writeState.optPostion);
		buffer.limit(proxyBuf.writeState.optLimit);
		int readed = channel.read(buffer);
		proxyBuf.writeState.curOptedLength = readed;
		if (readed > 0) {
			proxyBuf.writeState.optPostion += readed;
			proxyBuf.writeState.optedTotalLength += readed;
		}
		return readed;
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

	@SuppressWarnings("unchecked")
	public void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = ((normal) ? " normal close " : "abnormal close ")
				+ ((channel == frontChannel) ? " front socket. " : " backend socket. ");
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
		if (channel == frontChannel) {
			((FrontIOHandler) curProxyHandler).onFrontSocketClosed(this, normal);
			frontChannel = null;
		} else if (channel == frontChannel) {
			((BackendIOHandler) curProxyHandler).onBackendSocketClosed(this, normal);
			backendChannel = null;
		}

	}

	public void setCurProxyHandler(NIOHandler proxyHandler) {
		curProxyHandler = proxyHandler;
	}

	public NIOHandler getCurProxyHandler() {
		return curProxyHandler;
	}

	private void closeSocket(Channel channel) {
		if (channel != null && channel.isOpen()) {
			try {
				channel.close();
			} catch (IOException e) {
				//
			}

		}
	}

	public void addSessionAttr(String attrName, Object value) {
		logger.info("add session attr:" + attrName + " value:" + value);
	}

	public Map<String, Object> getSessionAttrMap() {
		return sessionAttrMap;
	}

	/**
	 * 根据网络模式确定是否修改前端连接与后端连接的NIO感兴趣事件
	 * 
	 * @param userSession
	 * @throws ClosedChannelException
	 */
	public void modifySelectKey() throws ClosedChannelException {
		boolean frontKeyNeedUpdate=false;
		boolean backKeyNeedUpdate=false;
		switch (netOptMode) {
		case DirectTrans: {
			frontKeyNeedUpdate = true;
			backKeyNeedUpdate = true;
			break;
		}
		case FrontRW: {
			frontKeyNeedUpdate = true;
			backKeyNeedUpdate = false;
			break;
		}
		case BackendRW: {
			frontKeyNeedUpdate = false;
			backKeyNeedUpdate = true;
			break;
		}
		}
		if (frontKeyNeedUpdate && frontKey != null && frontKey.isValid()) {
			int clientOps = 0;
			if (backendBuffer.isInWriting())
				clientOps |= SelectionKey.OP_READ;
			if (frontBuffer.isInWriting() == false)
				clientOps |= SelectionKey.OP_WRITE;
			frontKey.interestOps(clientOps);
		}
		if (backKeyNeedUpdate && backendKey != null && backendKey.isValid()) {
			int serverOps = 0;
			if (frontBuffer.isInWriting())
				serverOps |= SelectionKey.OP_READ;
			if (backendBuffer.isInWriting() == false)
				serverOps |= SelectionKey.OP_WRITE;
			backendKey.interestOps(serverOps);
		}
	}
}
