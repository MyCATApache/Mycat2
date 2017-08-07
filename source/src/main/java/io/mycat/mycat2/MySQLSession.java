package io.mycat.mycat2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mysql.Capabilities;
import io.mycat.mysql.Versions;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.proxy.BufferOptState;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.StringUtil;
import io.mycat.proxy.UserSession;
import io.mycat.util.ParseUtil;
import io.mycat.util.RandomUtil;

public class MySQLSession extends UserSession {

	/**
	 * 当前处理中的SQL报文的信息（前端）
	 */
	public MySQLPackageInf curFrontMSQLPackgInf = new MySQLPackageInf();
	public MySQLPackageInf curBackendMSQLPackgInf = new MySQLPackageInf();
	public SQLProcessor curSQLProcessor;
	public byte[] seed;

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		// boolean usingCompress = MycatServer.getInstance().getConfig()
		// .getSystem().getUseCompression() == 1;
		// if (usingCompress) {
		// flag |= Capabilities.CLIENT_COMPRESS;
		// }
		flag |= Capabilities.CLIENT_ODBC;
		flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		return flag;
	}

	public void sendAuthPackge() throws IOException {
		// 生成认证数据
		byte[] rand1 = RandomUtil.randomBytes(8);
		byte[] rand2 = RandomUtil.randomBytes(12);

		// 保存认证数据
		byte[] seed = new byte[rand1.length + rand2.length];
		System.arraycopy(rand1, 0, seed, 0, rand1.length);
		System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
		this.seed = seed;

		// 发送握手数据包
		HandshakePacket hs = new HandshakePacket();
		hs.packetId = 0;
		hs.protocolVersion = Versions.PROTOCOL_VERSION;
		hs.serverVersion = Versions.SERVER_VERSION;
		hs.threadId = this.sessionId;
		hs.seed = rand1;
		hs.serverCapabilities = getServerCapabilities();
		// hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
		hs.serverStatus = 2;
		hs.restOfScrambleBuff = rand2;
		hs.write(backendBuffer);
		backendBuffer.flip();
		this.writeToChannel(backendBuffer, this.frontChannel);
	}
	
	

	public MySQLSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) {
		super(bufPool, nioSelector, frontChannel);

	}

	public void setCurrentSQLProcessor(SQLProcessor sqlCmd) {
		curSQLProcessor = sqlCmd;
	}

	public SQLProcessor getCurSQLProcessor() {
		return curSQLProcessor;
	}

	public void pushSQLCmd(SQLProcessor sqlCmd) {

	}

	public void popSQLCmd() {

	}

	/**
	 * 解析MySQL报文，解析的结果存储在curMSQLPackgInf中，如果解析到完整的报文，就返回TRUE
	 * 
	 * @param proxyBuf
	 * @return
	 * @throws IOException
	 */
	public boolean resolveMySQLPackage(ProxyBuffer proxyBuf, MySQLPackageInf curPackInf) throws IOException {
		ByteBuffer buffer = proxyBuf.getBuffer();
		BufferOptState readState = proxyBuf.readState;
		int offset = readState.optPostion;
		int limit = readState.optLimit;
		int totalLen = limit - offset;
		if (curPackInf.crossBuffer) {
			if (curPackInf.remainsBytes <= totalLen) {
				// 跳过剩余的报文
				offset += curPackInf.remainsBytes;
				// 新报文开始
				curPackInf.crossBuffer = false;

			} else {// 剩余报文还没读完，等待下一次读取
				curPackInf.remainsBytes -= totalLen;
				return false;
			}
		}
		// 读取到了包头和长度
		// 是否讀完一個報文

		if (!ParseUtil.validateHeader(offset, limit)) {
			logger.debug("not read a whole packet ,session {},offset {} ,readable len {}", sessionId, offset, totalLen);
			return false;
		}

		int pkgLength = ParseUtil.getPacketLength(buffer, offset);
		// 解析报文类型
		final byte packetType = buffer.get(offset + ParseUtil.msyql_packetHeaderSize);
		curPackInf.pkgType = packetType;
		curPackInf.length = pkgLength;
		curPackInf.startPos = offset;
		if ((offset + pkgLength) > limit) {
			logger.debug("Not a whole packet: required length = {} bytes, cur total length = {} bytes, "
					+ "ready to handle the next read event", sessionId, buffer.hashCode(), pkgLength, limit);
			curPackInf.crossBuffer = true;
			curPackInf.remainsBytes = offset + pkgLength - limit;
			readState.optPostion = readState.optLimit;
			return false;
		} else {
			// 读到完整报文
			curPackInf.crossBuffer = false;
			curPackInf.remainsBytes = 0;
			if (ProxyRuntime.INSTANCE.isTraceProtocol()) {
				final String hexs = StringUtil.dumpAsHex(buffer, curPackInf.startPos, pkgLength);
				logger.info(
						" session {} received a packet: startPos={}, offset = {}, length = {}, type = {}, cur total length = {}, packet bytes\n{}",
						sessionId, curPackInf.startPos, offset, pkgLength, packetType, limit, hexs);

			}
			offset += pkgLength;
			readState.optPostion = offset;
			return true;
		}

	}

}
