package io.mycat.mycat2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.beans.MySQLCharset;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.beans.SchemaBean;
import io.mycat.mysql.Capabilities;
import io.mycat.mysql.Versions;
import io.mycat.mysql.packet.HandshakePacket;
import io.mycat.mysql.packet.MySQLPacket;
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
	/**
	 * 当前处理中的SQL报文的信息（后端）
	 */
	public MySQLPackageInf curBackendMSQLPackgInf = new MySQLPackageInf();

	/**
	 * 前端字符集
	 */
	public MySQLCharset frontCharSet;
	/**
	 * 前端用户
	 */
	public String clientUser;
	/**
	 * Mycat Schema
	 */
	public SchemaBean schema; 
	
		
	/**
	 * 认证中的seed报文数据
	 */
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

	/**
	 * 回应客户端（front或Sever）OK 报文。
	 * 
	 * @param pkg
	 *            ，必须要是OK报文或者Err报文
	 * @throws IOException
	 */
	public void responseOKOrError(MySQLPacket pkg, boolean front) throws IOException {
		if (front) {
			pkg.write(this.frontBuffer);
			frontBuffer.flip();
			this.writeToChannel(frontBuffer, this.frontChannel);
		} else {
			pkg.write(this.backendBuffer);
			backendBuffer.flip();
			this.writeToChannel(backendBuffer, this.backendChannel);
		}
	}

	/**
	 * 给客户端（front）发送认证报文
	 * 
	 * @throws IOException
	 */
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
		hs.write(this.frontBuffer);
		frontBuffer.flip();
		this.writeToChannel(frontBuffer, this.frontChannel);
	}

	public MySQLSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) {
		super(bufPool, nioSelector, frontChannel);

	}

	/**
	 * 从Socket中读取数据，通常在NIO事件中调用，比如onFrontRead/onBackendRead
	 * 
	 * @param session
	 * @param readFront
	 * @return
	 * @throws IOException
	 */
	public boolean readSocket(boolean readFront) throws IOException {
		ProxyBuffer buffer = backendBuffer;
		SocketChannel channel = frontChannel;
		if (!readFront) {
			buffer = frontBuffer;
			channel = backendChannel;
		}
		int readed = readFromChannel(buffer, channel);
		logger.debug("readed {} total bytes ", readed);
		if (readed == -1) {
			closeSocket(channel, true, "read EOF.");
			return false;
		} else if (readed == 0) {
			logger.warn("read 0 bytes ,try compact buffer " + (readFront ? " front " : "backend ") + " ,session Id :"
					+ sessionId);
			buffer.compact(true);
			// todo curMSQLPackgInf
			// 也许要对应的改变位置,如果curMSQLPackgInf是跨Package的，则可能无需改变信息
			// curPackInf.
			return false;
		}
		buffer.updateReadLimit();
		return true;
	}

	/**
	 * 解析MySQL报文，解析的结果存储在curMSQLPackgInf中，如果解析到完整的报文，就返回TRUE
	 * 如果解析的过程中同时要移动ProxyBuffer的readState位置，即标记为读过，后继调用开始解析下一个报文，则需要参数markReaded=true
	 * 
	 * @param proxyBuf
	 * @return
	 * @throws IOException
	 */
	public boolean resolveMySQLPackage(ProxyBuffer proxyBuf, MySQLPackageInf curPackInf, boolean markReaded)
			throws IOException {
		boolean readWholePkg = false;
		ByteBuffer buffer = proxyBuf.getBuffer();
		BufferOptState readState = proxyBuf.readState;
		int offset = readState.optPostion;
		int limit = readState.optLimit;
		int totalLen = limit - offset;
		if (totalLen == 0) {
			return false;
		}
		if (curPackInf.crossBuffer) {
			if (curPackInf.remainsBytes <= totalLen) {
				// 剩余报文结束
				curPackInf.endPos = offset + curPackInf.remainsBytes;
				curPackInf.remainsBytes = 0;
				readWholePkg = true;
			} else {// 剩余报文还没读完，等待下一次读取
				curPackInf.remainsBytes -= totalLen;
				curPackInf.endPos = limit;
				readWholePkg = false;
			}
		} else if (!ParseUtil.validateHeader(offset, limit)) {
			logger.debug("not read a whole packet ,session {},offset {} ,limit {}", sessionId, offset, limit);
			readWholePkg = false;
		}

		int pkgLength = ParseUtil.getPacketLength(buffer, offset);
		// 解析报文类型
		final byte packetType = buffer.get(offset + ParseUtil.msyql_packetHeaderSize);
		curPackInf.pkgType = packetType;
		curPackInf.pkgLength = pkgLength;
		curPackInf.startPos = offset;
		curPackInf.crossBuffer = false;
		curPackInf.remainsBytes = 0;
		if ((offset + pkgLength) > limit) {
			logger.debug("Not a whole packet: required length = {} bytes, cur total length = {} bytes, "
					+ "ready to handle the next read event", sessionId, buffer.hashCode(), pkgLength, limit);
			curPackInf.crossBuffer = true;
			curPackInf.remainsBytes = offset + pkgLength - limit;
			curPackInf.endPos = limit;
			readWholePkg = false;
		} else {
			// 读到完整报文
			curPackInf.endPos = curPackInf.pkgLength + curPackInf.startPos;
			if (ProxyRuntime.INSTANCE.isTraceProtocol()) {
				/**
				 * @todo 跨多个报文的情况下，修正错误。
				 */
				final String hexs = StringUtil.dumpAsHex(buffer, curPackInf.startPos, curPackInf.pkgLength);
				logger.info(
						"     session {} packet: startPos={}, offset = {}, length = {}, type = {}, cur total length = {},pkg HEX\r\n {}",
						sessionId, curPackInf.startPos, offset, pkgLength, packetType, limit, hexs);
			}
			readWholePkg = true;
		}
		if (markReaded) {
			readState.optPostion = curPackInf.endPos;
		}
		return readWholePkg;

	}

}
