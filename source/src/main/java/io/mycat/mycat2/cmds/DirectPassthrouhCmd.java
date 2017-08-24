package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class DirectPassthrouhCmd implements SQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(DirectPassthrouhCmd.class);

	public static final DirectPassthrouhCmd INSTANCE = new DirectPassthrouhCmd();

	// ********** 临时处理,等待与KK 代码合并
	private static final Map<Byte, Integer> finishPackage = new HashMap<>();

	private Map<Byte, Integer> curfinishPackage = new HashMap<>();

	static {
		finishPackage.put(MySQLPacket.OK_PACKET, 1);
		finishPackage.put(MySQLPacket.ERROR_PACKET, 1);
		finishPackage.put(MySQLPacket.EOF_PACKET, 2);
	}
	// ********** 临时处理,等待与KK 代码合并

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		curfinishPackage.putAll(finishPackage);
		ProxyBuffer curBuffer = session.proxyBuffer;
		// 切换 buffer 读写状态
		curBuffer.flip();
		// 没有读取,直接透传时,需要指定 透传的数据 截止位置
		curBuffer.readIndex = curBuffer.writeIndex;
		// 改变 owner，对端Session获取，并且感兴趣写事件
		session.giveupOwner(SelectionKey.OP_WRITE);
		session.getBackend().writeToChannel();
		return false;
	}

	private boolean isfinishPackage(MySQLPackageInf curMSQLPackgInf) throws IOException {
		switch (curMSQLPackgInf.pkgType) {
		case MySQLPacket.OK_PACKET:
		case MySQLPacket.ERROR_PACKET:
		case MySQLPacket.EOF_PACKET:
			return true;
		default:
			return false;
		}
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		logger.info("received backend mysql data ");
		if (!session.readFromChannel()) {
			return false;
		}

		ProxyBuffer curBuffer = session.proxyBuffer;
		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;
		boolean isallfinish = false;
		boolean isContinue = true;
		while (isContinue) {
			switch (session.resolveMySQLPackage(curBuffer, curMSQLPackgInf, true)) {
			case Full:
				Integer count = curfinishPackage.get(curMSQLPackgInf.pkgType);
				if (count != null) {
					if (--count == 0) {
						isallfinish = true;
						curfinishPackage.clear();
					}
					curfinishPackage.put(curMSQLPackgInf.pkgType, count);
				}
				if (curBuffer.readIndex == curBuffer.writeIndex) {
					isContinue = false;
				} else {
					isContinue = true;
				}
				break;
			case LongHalfPacket:
				if (curMSQLPackgInf.crossBuffer) {
					// 发生过透传的半包,往往包的长度超过了buffer 的长度.
					logger.debug(" readed crossBuffer LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
				} else if (!isfinishPackage(curMSQLPackgInf)) {
					// 不需要整包解析的长半包透传. result set .这种半包直接透传
					curMSQLPackgInf.crossBuffer = true;
					curBuffer.readIndex = curMSQLPackgInf.endPos;
					curMSQLPackgInf.remainsBytes = curMSQLPackgInf.pkgLength
							- (curMSQLPackgInf.endPos - curMSQLPackgInf.startPos);
					logger.debug(" readed LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
					logger.debug(" curBuffer {}", curBuffer);
				} else {
					// 读取到了EOF/OK/ERROR 类型长半包 是需要保证是整包的.
					logger.debug(" readed finished LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
					// TODO 保证整包的机制
				}
				isContinue = false;
				break;
			case ShortHalfPacket:
				logger.debug(" readed ShortHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
				isContinue = false;
				break;
			}
		}
		;

		// 切换buffer 读写状态
		curBuffer.flip();
		MycatSession mycatSession = session.getMycatSession();
		// 直接透传报文
		mycatSession.takeOwner(SelectionKey.OP_WRITE);
		mycatSession.writeToChannel();
		/**
		 * 当前命令处理是否全部结束,全部结束时需要清理资源
		 */
		return false;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		// 判断是否结果集传输完成，决定命令是否结束，切换到前端读取数据
		// todo
		logger.warn("not well implemented ,please fix it ");
		session.proxyBuffer.flip();
		session.chnageBothReadOpts();
		return false;

	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// 绝大部分情况下，前端把数据写完后端发送出去后，就等待后端返回数据了，
		// 此时Buffer改为读状态
		session.proxyBuffer.flip();
		session.change2ReadOpts();
		return false;

	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {

		return true;
	}

}
