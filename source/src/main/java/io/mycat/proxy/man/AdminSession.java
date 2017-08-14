package io.mycat.proxy.man;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.BufferOptState;
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

	public AdminSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel);

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
		frontBuffer.reset();
		packet.writeTo(frontBuffer);
		frontBuffer.flip();
		this.writeToChannel(frontBuffer, frontChannel);

	}

	/**
	 * 解析请求报文，如果解析到完整的报文，就返回此报文的类型。否则返回-1
	 * 
	 * @return
	 * @throws IOException
	 */
	public byte receivedPacket() throws IOException {
		ByteBuffer buffer = frontBuffer.getBuffer();
		BufferOptState readState = frontBuffer.readState;
		int offset = readState.optPostion;
		int limit = readState.optLimit;
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
					"     session {} packet: startPos={}, offset = {}, length = {}, type = {}, cur total length = {},pkg HEX\r\n {}",
					getSessionId(), 0, offset, pkgLength, packetType, limit, hexs);
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
		int readed = readFromChannel(this.frontBuffer, this.frontChannel);
		logger.debug("readed {} total bytes ", readed);
		if (readed == -1) {
			closeSocket(frontChannel, true, "read EOF.");
			return false;
		} else if (readed == 0) {
			logger.warn("read 0 bytes ,try compact buffer  ,session Id :" + this.getSessionId());
			frontBuffer.compact(true);
			return false;
		}
		frontBuffer.updateReadLimit();
		return true;
	}

	public void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = ((normal) ? " normal close " : "abnormal close " + " socket ");
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
		((FrontIOHandler<Session>)this.getCurNIOHandler()).onFrontSocketClosed(this, normal);

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
