package io.mycat.mycat2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.beans.MySQLCharset;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.AutoCommit;
import io.mycat.mysql.Isolation;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.AbstractSession;
import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.ParseUtil;
import io.mycat.util.StringUtil;

/**
 * 抽象的MySQL的连接会话
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractMySQLSession extends AbstractSession {

	// 当前接收到的包类型
	public enum CurrPacketType {
		Full, LongHalfPacket, ShortHalfPacket
	}

	/**
	 * 字符集
	 */
	public MySQLCharset charSet;
	/**
	 * 用户
	 */
	public String clientUser;

	/**
	 * 事务隔离级别
	 */
	public Isolation isolation = Isolation.REPEATED_READ;

	/**
	 * 事务提交方式
	 */
	public AutoCommit autoCommit = AutoCommit.ON;
	
	/**
	 * 认证中的seed报文数据
	 */
	public byte[] seed;
	/**
	 * 当前处理中的SQL报文的信息
	 */
	public MySQLPackageInf curMSQLPackgInf = new MySQLPackageInf();

	public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		this(bufferPool, selector, channel, SelectionKey.OP_READ);

	}

	public AbstractMySQLSession(BufferPool bufferPool, Selector selector, SocketChannel channel, int keyOpt)
			throws IOException {
		super(bufferPool, selector, channel, keyOpt);

	}

	public void setCurBufOwner(boolean curBufOwner) {
		this.curBufOwner = curBufOwner;
	}

	/**
	 * 回应客户端（front或Sever）OK 报文。
	 *
	 * @param pkg
	 *            ，必须要是OK报文或者Err报文
	 * @throws IOException
	 */
	public void responseOKOrError(MySQLPacket pkg) throws IOException {
		// proxyBuffer.changeOwner(true);
		this.proxyBuffer.reset();
		pkg.write(this.proxyBuffer);
		proxyBuffer.flip();
		proxyBuffer.readIndex = proxyBuffer.writeIndex;
		this.writeToChannel();
	}

	/**
	 * 解析MySQL报文，解析的结果存储在curMSQLPackgInf中，如果解析到完整的报文，就返回TRUE
	 * 如果解析的过程中同时要移动ProxyBuffer的readState位置，即标记为读过，后继调用开始解析下一个报文，则需要参数markReaded
	 * =true
	 *
	 * @param proxyBuf
	 * @return
	 * @throws IOException
	 */
	public CurrPacketType resolveMySQLPackage(ProxyBuffer proxyBuf, MySQLPackageInf curPackInf, boolean markReaded)
			throws IOException {

		ByteBuffer buffer = proxyBuf.getBuffer();
		// 读取的偏移位置
		int offset = proxyBuf.readIndex;
		// 读取的总长度
		int limit = proxyBuf.writeIndex;
		// 读取当前的总长度
		int totalLen = limit - offset;
		if (totalLen == 0) { // 透传情况下. 如果最后一个报文正好在buffer 最后位置,已经透传出去了.这里可能不会为零
			return CurrPacketType.ShortHalfPacket;
		}

		if (curPackInf.remainsBytes == 0 && curPackInf.crossBuffer) {
			curPackInf.crossBuffer = false;
		}

		// 如果当前跨多个报文
		if (curPackInf.crossBuffer) {
			if (curPackInf.remainsBytes <= totalLen) {
				// 剩余报文结束
				curPackInf.endPos = offset + curPackInf.remainsBytes;
				offset += curPackInf.remainsBytes; // 继续处理下一个报文
				proxyBuf.readIndex = offset;
				curPackInf.remainsBytes = 0;
			} else {// 剩余报文还没读完，等待下一次读取
				curPackInf.startPos = 0;
				curPackInf.remainsBytes -= totalLen;
				curPackInf.endPos = limit;
				proxyBuf.readIndex = curPackInf.endPos;
				return CurrPacketType.LongHalfPacket;
			}
		}
		// 验证当前指针位置是否
		if (!ParseUtil.validateHeader(offset, limit)) {
			// 收到短半包
			logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset, limit);
			return CurrPacketType.ShortHalfPacket;
		}

		// 解包获取包的数据长度
		int pkgLength = ParseUtil.getPacketLength(buffer, offset);
		// 解析报文类型
		// final byte packetType = buffer.get(offset +
		// ParseUtil.msyql_packetHeaderSize);

		// 解析报文类型
		int packetType = -1;

		// 在包长度小于7时，作为resultSet的首包
		if (pkgLength <= 7) {
			int index = offset + ParseUtil.msyql_packetHeaderSize;

			long len = proxyBuf.getInt(index, 1) & 0xff;
			// 如果长度小于251,则取默认的长度
			if (len < 251) {
				packetType = (int) len;
			} else if (len == 0xfc) {
				// 进行验证是否位数足够,作为短包处理
				if (!ParseUtil.validateResultHeader(offset, limit, 2)) {
					// 收到短半包
					logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
							limit);
					return CurrPacketType.ShortHalfPacket;
				}
				packetType = (int) proxyBuf.getInt(index + 1, 2);
			} else if (len == 0xfd) {

				// 进行验证是否位数足够,作为短包处理
				if (!ParseUtil.validateResultHeader(offset, limit, 3)) {
					// 收到短半包
					logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
							limit);
					return CurrPacketType.ShortHalfPacket;
				}

				packetType = (int) proxyBuf.getInt(index + 1, 3);
			} else {
				// 进行验证是否位数足够,作为短包处理
				if (!ParseUtil.validateResultHeader(offset, limit, 8)) {
					// 收到短半包
					logger.debug("not read a whole packet ,session {},offset {} ,limit {}", getSessionId(), offset,
							limit);
					return CurrPacketType.ShortHalfPacket;
				}

				packetType = (int) proxyBuf.getInt(index + 1, 8);
			}
		} else {
			// 解析报文类型
			packetType = buffer.get(offset + ParseUtil.msyql_packetHeaderSize);
		}

		// 包的类型
		curPackInf.pkgType = packetType;
		// 设置包的长度
		curPackInf.pkgLength = pkgLength;
		// 设置偏移位置
		curPackInf.startPos = offset;

		curPackInf.crossBuffer = false;

		curPackInf.remainsBytes = 0;
		// 如果当前需要跨buffer处理
		if ((offset + pkgLength) > limit) {
			logger.debug("Not a whole packet: required length = {} bytes, cur total length = {} bytes, limit ={}, "
					+ "ready to handle the next read event", pkgLength, (limit - offset), limit);
			curPackInf.endPos = limit;
			return CurrPacketType.LongHalfPacket;
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
						getSessionId(), curPackInf.startPos, offset, pkgLength, packetType, limit, hexs);
			}
			if (markReaded) {
				proxyBuf.readIndex = curPackInf.endPos;
			}
			return CurrPacketType.Full;
		}
	}

}
