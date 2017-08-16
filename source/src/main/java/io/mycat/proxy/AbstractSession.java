package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
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

	// Session是否关闭
	private boolean closed;
	
	//当前接收到的包类型
	public enum CurrPacketType{
		Full,LongHalfPacket,ShortHalfPacket
	}

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
	public void close(boolean normal,String hint) {
		if (!this.isClosed()) {
			this.closed = true;
			logger.info("close session " + this.sessionInfo() + " for reason " + hint);
			closeSocket(frontChannel,normal,hint);
			this.getMySessionManager().removeSession(this);
		} else {
			logger.warn("session already closed " + this.sessionInfo());
		}

	}

	protected void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = (normal) ? " normal close " : "abnormal close "+channel;
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
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
