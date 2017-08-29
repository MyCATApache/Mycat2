package io.mycat.mycat2.cmds.pkgread;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.cmds.judge.EofJudge;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 
 * 仅进行查询结果的命令处理
 * 
 * @author liujun
 * @version 1.0.0
 * @since 2017年8月22日 下午4:13:07
 */
public class PkgResultSetReader implements PkgProcess {

	private static final Logger logger = LoggerFactory.getLogger(PkgResultSetReader.class);

	public static final PkgResultSetReader INSTANCE = new PkgResultSetReader();

	/**
	 * 后端报文处理
	 * 
	 * @param session
	 * @return
	 * @throws IOException
	 */
	public boolean procssPkg(MySQLSession session) throws IOException {

		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;

		ProxyBuffer curBuffer = session.proxyBuffer;

		boolean isContinue = true;
		boolean isFinish = false;

		while (isContinue) {
			// 进行报文的读取操作
			switch (session.resolveMySQLPackage(curBuffer, curMSQLPackgInf, true)) {
			// 如果当前为整包
			case Full:
				// 检查当前是否为eof包,并且为整包 ,解析eof包
				if (session.curMSQLPackgInf.pkgType == MySQLPacket.EOF_PACKET) {
					// 首先检查当前列标识结果
					if (!session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_KEY_COLUMN_OVER.getKey())) {
						session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_COLUMN_OVER.getKey(), true);
					}
					// 如果当前列列结束，则进行结束标识验证
					else {
						// 进行标识重置
						session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_COLUMN_OVER.getKey());
						isFinish = true;

						// 如果当前的eof包大于1说明已经为eof结束包,切换到解析器进行解析
						boolean gotoRead = EofJudge.INSTANCE.judge(session);

						// 当一个完整的查询检查结束后，切换至首包的检查
						session.currPkgProc = PkgFirstReader.INSTANCE;

						// 检查是否需要读取下一个包
						if (gotoRead) {
							// 并且为直接返回
							return true;
						}
					}
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

		// 标识当前传输未结束

		// 切换buffer 读写状态
		curBuffer.flip();
		MycatSession mycatSession = session.getMycatSession();
		// 直接透传报文
		mycatSession.takeOwner(SelectionKey.OP_WRITE);

		if (!isFinish) {
			// 标识当前传输未结束
			mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey(), true);
		}

		mycatSession.writeToChannel();

		/**
		 * 当前命令处理是否全部结束,全部结束时需要清理资源
		 */
		return false;
	}

	/**
	 * 进行当前完成包验证
	 * 
	 * @param curMSQLPackgInf
	 * @return
	 * @throws IOException
	 */
	private boolean isfinishPackage(MySQLPackageInf curMSQLPackgInf) throws IOException {
		switch (curMSQLPackgInf.pkgType) {
		case MySQLPacket.EOF_PACKET:
			return true;
		default:
			return false;
		}
	}

}
