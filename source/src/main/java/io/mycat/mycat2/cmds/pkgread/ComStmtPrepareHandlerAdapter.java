package io.mycat.mycat2.cmds.pkgread;

import java.io.IOException;

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
 * 0x16 COM_STMT_PREPARE 预处理SQL语句
 * 
 * @since 2017年8月23日 下午11:09:49
 * @version 0.0.1
 * @author liujun
 */
public class ComStmtPrepareHandlerAdapter implements CommandHandlerAdapter {

	private static final Logger logger = LoggerFactory.getLogger(ComStmtPrepareHandlerAdapter.class);

	/**
	 * 首包处理的实例对象
	 */
	public static final ComStmtPrepareHandlerAdapter INSTANCE = new ComStmtPrepareHandlerAdapter();

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
					boolean gotoRead = EofJudge.INSTANCE.judge(session);

					// 检查是否需要读取下一个包
					if (gotoRead) {
						// 并且为直接返回
						return true;
					}
				}

				if (curBuffer.readIndex == curBuffer.writeIndex) {
					isContinue = false;
				} else {
					isContinue = true;
				}
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

		// 切换buffer 读写状态
		// curBuffer.flip();
		MycatSession mycatSession = session.getMycatSession();
		// 直接透传报文
		// mycatSession.takeOwner(SelectionKey.OP_WRITE);

		if (!isFinish) {
			// 标识当前传输未结束
			mycatSession.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey(), true);
		} else {
			// 结束移除标识
			mycatSession.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey());
		}

		// mycatSession.writeToChannel();

		/**
		 * 当前命令处理是否全部结束,全部结束时需要清理资源
		 */
		return false;
	}

}
