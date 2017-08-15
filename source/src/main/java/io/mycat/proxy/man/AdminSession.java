package io.mycat.proxy.man;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.FrontIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.Session;

/**
 * Mycat各个节点发起会话,规定Node name大的节点主动向Node Name节点小的发起连接请求， 比如 mycat-server-1，
 * mycat-server-2，mycat-server-3, 2与3 都向1发起连接
 * 
 * @author wuzhihui
 *
 */
public class AdminSession extends AbstractSession {

	private String nodeId;
	public AdminCommand curAdminCommand;
	// 全双工模式，读写用两个不同的Buffer,不会相互切换
	public ProtocolBuffer readingBuffer;
	public ProtocolBuffer writingBuffer;
	public PackageInf curAdminPkgInf = new PackageInf();

	public AdminSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel);
		this.readingBuffer = new ProtocolBuffer(bufferPool.allocByteBuffer());
		this.writingBuffer = new ProtocolBuffer(bufferPool.allocByteBuffer());

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
		if (frontKey != null && frontKey.isValid()) {
			int clientOps = SelectionKey.OP_READ;
			if (writingBuffer.optLimit == writingBuffer.optMark) {
				this.writingBuffer.reset();
				clientOps &= ~SelectionKey.OP_WRITE;
			} else {
				clientOps |= SelectionKey.OP_WRITE;
			}
			frontKey.interestOps(clientOps);
		}
	}

	public void writeChannel() throws IOException {
		// 尝试压缩，移除之前写过的内容
		ByteBuffer buffer = writingBuffer.getBuffer();
		if (writingBuffer.optMark > buffer.capacity() * 2 / 3) {
			buffer.limit(writingBuffer.optLimit);
			buffer.position(writingBuffer.optMark);
			buffer.compact();
			writingBuffer.optMark = 0;
			writingBuffer.optLimit = buffer.position();
		}
		buffer.limit(writingBuffer.optLimit);
		buffer.position(writingBuffer.optMark);
		int writed = this.frontChannel.write(buffer);
		if (writed > 0) {
			writingBuffer.optMark = buffer.position();
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
		int offset = readingBuffer.optMark;
		int limit = readingBuffer.optLimit;
		if(limit==offset)
		{
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
					"     session {} packet:  offset = {}, length = {}, type = {}, cur total length = {},pkg HEX\r\n {}",
					getSessionId(), offset, pkgLength, packetType, limit, hexs);
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
		if (readingBuffer.optMark > buffer.capacity() * 1 / 3) {
			buffer.limit(readingBuffer.optLimit);
			buffer.position(readingBuffer.optMark);
			buffer.compact();
			readingBuffer.optMark = 0;
		} else {
			buffer.position(readingBuffer.optLimit);
		}
		int readed = frontChannel.read(buffer);
		logger.debug(" readed {} total bytes ", readed);
		if (readed == -1) {
			logger.warn("Read EOF ,socket closed ");
			throw new ClosedChannelException();
		} else if (readed == 0) {

			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		readingBuffer.optLimit = buffer.position();
		return readed > 0;
	}

	public void close(boolean normal, String hint) {
		if (!this.isClosed()) {
			bufPool.recycleBuf(this.readingBuffer.getBuffer());
			bufPool.recycleBuf(this.writingBuffer.getBuffer());
			super.close(normal, hint);
		} else {
			super.close(normal, hint);
		}

	}

	protected void closeSocket(SocketChannel channel, boolean normal, String msg) {
		super.closeSocket(channel, normal, msg);
		((FrontIOHandler<Session>) this.getCurNIOHandler()).onFrontSocketClosed(this, normal);

	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public SocketChannel frontChannel() {
		return this.frontChannel;
	}

}
