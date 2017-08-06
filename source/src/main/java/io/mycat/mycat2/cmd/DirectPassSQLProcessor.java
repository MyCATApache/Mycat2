package io.mycat.mycat2.cmd;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLPackageInf;
import io.mycat.mycat2.MySQLSession;
import io.mycat.proxy.BufferOptState;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.StringUtil;

public class DirectPassSQLProcessor extends AbstractSQLProcessor {
	private static Logger logger = LoggerFactory.getLogger(DirectPassSQLProcessor.class);
	public static DirectPassSQLProcessor INSTANCE = new DirectPassSQLProcessor();
	// private ArrayList<Runnable> pendingJob;

	@Override
	public boolean handFrontPackage(MySQLSession userSession) throws IOException {
		MySQLPackageInf curPackInf = userSession.curMSQLPackgInf;
		int readed = userSession.backendBuffer.writeFromChannel(userSession.frontChannel);
		if (readed == -1) {
			this.closeSocket(userSession, userSession.frontChannel, true, "read EOF.");
		} else if (readed == 0) {
			logger.info("read 0 bytes ,try compact buffer ,session Id :" + userSession.sessionId);
			userSession.backendBuffer.compact();

			// todo curMSQLPackgInf
			// 也许要对应的改变位置,如果curMSQLPackgInf是跨Package的，则可能无需改变信息
			// curPackInf.
			return false;
		}
		ProxyBuffer proxyBuf = userSession.backendBuffer;
		BufferOptState writeState = proxyBuf.getWriteOptState();
		ByteBuffer buffer = proxyBuf.getBuffer();
		int offset = writeState.markPos;
		int limit = writeState.optPostion;
		int readedPackge=0;
		while (true) {
			if (curPackInf.crossBuffer) {
				if (curPackInf.remainsBytes <= writeState.curOptedLength) {
					// 跳过剩余的报文
					offset += curPackInf.remainsBytes;
					// 新报文开始
					curPackInf.crossBuffer = false;

				} else {// 剩余报文还没读完，等待下一次读取
					curPackInf.remainsBytes -= writeState.curOptedLength;
					writeState.markPos = writeState.optPostion;
					return (readedPackge>0);
				}
			}
			// 读取到了包头和长度
			// 是否讀完一個報文

			if (!validateHeader(offset, limit)) {
				logger.debug("not read a whole packet " + userSession.sessionId);
				return readedPackge>0;
			}

			int length = getPacketLength(buffer, offset);
			// 解析报文类型
			final byte packetType = buffer.get(offset + msyql_packetHeaderSize);
			curPackInf.pkgType = packetType;
			curPackInf.length = length;
			curPackInf.startPos = offset;
			if ((offset + length) > limit) {
				logger.debug(
						"C#{}B#{} nNot a whole packet: required length = {} bytes, cur total length = {} bytes, "
								+ "ready to handle the next read event",
						userSession.sessionId, buffer.hashCode(), length, limit);
				curPackInf.crossBuffer = true;
				curPackInf.remainsBytes = offset + length - limit;
				writeState.markPos = writeState.optPostion;
				return readedPackge>0;
			} else {
				curPackInf.crossBuffer = false;
				curPackInf.remainsBytes = 0;
				if (ProxyRuntime.INSTANCE.isTraceProtocol()) {
					final String hexs = StringUtil.dumpAsHex(buffer, curPackInf.startPos, length);
					logger.info(
							"C#{}B#{} received a packet: offset = {}, length = {}, type = {}, cur total length = {}, packet bytes\n{}",
							userSession.sessionId, buffer.hashCode(), curPackInf.startPos, length, packetType, limit,
							hexs);

				}
			}
			writeState.markPos = offset;
			offset += length;
			readedPackge++;
		}
		
	}

	@Override
	public boolean handBackendPackage(MySQLSession session) throws IOException {
		int readed = session.frontBuffer.writeFromChannel(session.backendChannel);
		if (readed == -1) {
			this.closeSocket(session, session.backendChannel, true, "read EOF.");
		} else if (readed == 0) {
			logger.info("read 0 bytes ,try compact buffer ,session Id :" + session.sessionId);
			session.frontBuffer.compact();
			
		}
		return true;
	}

}
