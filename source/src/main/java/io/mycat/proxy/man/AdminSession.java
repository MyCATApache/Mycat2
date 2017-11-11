package io.mycat.proxy.man;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.Session;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;

/**
 * Mycat各个节点发起会话,规定Node name大的节点主动向Node Name节点小的发起连接请求， 比如 mycat-server-1，
 * mycat-server-2，mycat-server-3, 2与3 都向1发起连接
 * 
 * @author wuzhihui
 *
 */
public class AdminSession implements Session {
	
	private SessionManager<AdminSession> sessionManager;
	public BufferPool bufPool;
	public Selector nioSelector;
	// 操作的Socket连接
	public String addr;
	public SocketChannel channel;
	public SelectionKey channelKey;
	protected static Logger logger = LoggerFactory.getLogger(AdminSession.class);
	private NIOHandler<AdminSession> nioHandler;
	private int sessionId;
	// Session是否关闭
	private boolean closed;
	private String nodeId;
	public AdminCommand curAdminCommand;
	// 全双工模式，读写用两个不同的Buffer,不会相互切换
	public ProxyBuffer readingBuffer;
	public ProxyBuffer writingBuffer;
	public PackageInf curAdminPkgInf = new PackageInf();

	public int confCount;

	public AdminSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this.bufPool = bufferPool;
		this.nioSelector = selector;
		this.channel = channel;
		InetSocketAddress clientAddr = (InetSocketAddress) channel.getRemoteAddress();
		this.addr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		SelectionKey socketKey = channel.register(nioSelector, SelectionKey.OP_READ, this);
		this.channelKey = socketKey;
		this.sessionId = ProxyRuntime.INSTANCE.genSessionId();
		this.readingBuffer = new ProxyBuffer(bufferPool.allocate());
		this.writingBuffer = new ProxyBuffer(bufferPool.allocate());

	}

	public MyCluster cluster() {
		return ProxyRuntime.INSTANCE.getMyCLuster();
	}

	/**
	 * 把报文写入到前端Buffer中并立即发送出去
	 * 
	 * @param packet
	 * @throws IOException
	 */
	public void answerClientNow(ManagePacket packet) throws IOException {
		writingBuffer.getBuffer().limit(writingBuffer.getBuffer().capacity());
		packet.writeTo(writingBuffer);
		this.writeChannel();
	}

	public void modifySelectKey() throws ClosedChannelException {
		if (channelKey != null && channelKey.isValid()) {
			int clientOps = SelectionKey.OP_READ;
			if (writingBuffer.readMark == writingBuffer.readIndex) {
				this.writingBuffer.reset();
				clientOps &= ~SelectionKey.OP_WRITE;
			} else {
				clientOps |= SelectionKey.OP_WRITE;
			}
			channelKey.interestOps(clientOps);
		}
	}

	public void writeChannel() throws IOException {
		// 尝试压缩，移除之前写过的内容
		ByteBuffer buffer = writingBuffer.getBuffer();
		if (writingBuffer.readIndex > buffer.capacity() * 2 / 3) {
			writingBuffer.compact();
		}else{
			buffer.limit(writingBuffer.readIndex);
			buffer.position(writingBuffer.readMark);
		}
		int writed = 0;
		try {
			writed = this.channel.write(buffer);
		} catch(IOException e){
			closeSocket(false,"Read EOF ,socket closed ");
		}
		if (writed > 0) {
			writingBuffer.readMark += writed;
		}
		modifySelectKey();
	}

	/**
	 * 解析请求报文，如果解析到完整的报文，就返回此报文的类型。否则返回-1
	 * 
	 * @return
	 * @throws IOException
	 */
	public byte receivedPacket() throws IOException {
		ByteBuffer buffer = this.readingBuffer.getBuffer();
		int offset = readingBuffer.readIndex;
		int limit = readingBuffer.writeIndex;
		if (limit == offset) {
			return -1;
		}
		if (!ManagePacket.validateHeader(offset, limit)) {
			logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset, limit);
			return -1;
		}
		int pkgLength = ManagePacket.getPacketLength(buffer, offset);

		if ((offset + pkgLength) > limit) {
			throw new RuntimeException("packet size too large!!" + pkgLength);
		} else {
			// 读到完整报文
			// 解析报文类型
			final byte packetType = buffer.get(offset + ManagePacket.packetHeaderSize - 1);
			final String hexs = io.mycat.util.StringUtil.dumpAsHex(buffer, 0, pkgLength);
			logger.info(
					"     session {} packet:  offset = {}, length = {}, type = {}, {} cur total length = {},pkg HEX\r\n {}",
					getSessionId(), offset, pkgLength, packetType, ManagePacket.getTypeString(packetType), limit, hexs);
			curAdminPkgInf.pkgType = packetType;
			curAdminPkgInf.length = pkgLength;
			curAdminPkgInf.startPos = offset;
			return packetType;
		}
	}

	/**
	 * 从Socket中读取数据，通常在NIO事件中调用，比如onFrontRead/onBackendRead
	 * 
	 * @param session
	 * @param readFront
	 * @return
	 * @throws IOException
	 */
	public boolean readSocket() throws IOException {

		// 尝试压缩，移除之前读过的内容
		ByteBuffer buffer = readingBuffer.getBuffer();
		if (readingBuffer.readIndex > buffer.capacity() * 1 / 3) {
			buffer.limit(readingBuffer.writeIndex);
			buffer.position(readingBuffer.readIndex);
			buffer.compact();
			readingBuffer.readIndex = 0;
		} else {
			buffer.position(readingBuffer.writeIndex);
		}
		int readed =0;
		try {
			readed = channel.read(buffer);
			logger.debug(" readed {} total bytes ", readed);
		} catch(IOException e){
			closeSocket(false,"Read EOF ,socket closed ");
		}
		if (readed == -1) {
			closeSocket(false,"Read EOF ,socket closed ");
		} else if (readed == 0) {

			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		readingBuffer.writeIndex = buffer.position();
		
		return readed > 0;
	}
	
	private void closeSocket(boolean normal,String msg) throws IOException{
		close(false,msg);
		throw new ClosedChannelException();
	}

	public void close(boolean normal, String hint) {
		if (!this.isClosed()) {
			this.closed = true;
			logger.info("close session " + this.sessionInfo() + " for reason " + hint);
			this.getMySessionManager().removeSession(this);
			closeSocket(channel, normal, hint);
			if(readingBuffer!=null){
				bufPool.recycle(readingBuffer.getBuffer());
				this.readingBuffer = null;
			}
			if(writingBuffer!=null){
				bufPool.recycle(writingBuffer.getBuffer());
				this.writingBuffer = null;
			}
		} else {
			logger.warn("session already closed " + this.sessionInfo());
		}
	}
	public String sessionInfo() {
		return " [" + this.addr + ']';
	}

	protected void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = (normal) ? " normal close " : "abnormal close " + channel;
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
		this.getCurNIOHandler().onSocketClosed(this, normal);

	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public SocketChannel channel() {
		return this.channel;
	}
	public int getSessionId() {
		return sessionId;
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

	

	@SuppressWarnings("unchecked")
	public void setSessionManager(SessionManager<? extends Session> sessionManager) {
		this.sessionManager = (SessionManager<AdminSession>) sessionManager;
	}
	public boolean isChannelOpen() {
		return channel != null && channel.isConnected();
	}
	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Session> SessionManager<T> getMySessionManager() {
		return (SessionManager<T>) sessionManager;
	}

	@Override
	public NIOHandler<AdminSession> getCurNIOHandler() {
	 return this.nioHandler;
	}

	public void setCurNIOHandler(NIOHandler<AdminSession> instance) {
		this.nioHandler=instance;
		
	}

}
