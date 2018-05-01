package io.mycat.mycat2.cmds.pkgread;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 
 * 0x16 COM_STMT_PREPARE 预处理SQL语句
 * 
 * @since 2017年8月23日 下午11:09:49
 * @version 0.0.1
 * @author liujun
 */
public class ComStmtPrepareHandler implements CommandHandler {

	private static final Logger logger = LoggerFactory.getLogger(ComStmtPrepareHandler.class);

	/**
	 * 首包处理的实例对象
	 */
	public static final ComStmtPrepareHandler INSTANCE = new ComStmtPrepareHandler();

	@Override
	public boolean procss(MySQLSession session) throws IOException {

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
					// 进行标识重置
					session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_COLUMN_OVER.getKey());
					isFinish = true;
					// 如果当前的eof包大于1说明已经为eof结束包,切换到解析器进行解析
                    boolean gotoRead = JudgeUtil.judgeEOFPacket(session, session.proxyBuffer);

					// 检查是否需要读取下一个包
					if (gotoRead) {
						// 并且为直接返回
						return true;
					}
				}

                isContinue = curBuffer.readIndex != curBuffer.writeIndex;
				break;

			case LongHalfPacket:
				// System.out.println("长半包:" + curMSQLPackgInf.seq);
				if (curMSQLPackgInf.crossBuffer) {
					// 发生过透传的半包,往往包的长度超过了buffer 的长度.
					logger.debug(" readed crossBuffer LongHalfPacket ,curMSQLPackgInf is {}", curMSQLPackgInf);
				} else if (curMSQLPackgInf.pkgType != MySQLPacket.EOF_PACKET) {
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
		MycatSession mycatSession = session.getMycatSession();

		if (!isFinish) {
			// 在stmt的处理中分为两阶段，首先进行SQL的预编译，然后进行值的执行,所以不能标识结束
			mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey(), true);
		}
		// 完成传输，则移除标识
		else {
			mycatSession.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey());
		}

		/**
		 * 当前命令处理是否全部结束,全部结束时需要清理资源
		 */
		return false;
	}

}
