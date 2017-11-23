package io.mycat.mycat2.cmds.pkgread;

import java.io.IOException;

import io.mycat.mycat2.AbstractMySQLSession.CurrPacketType;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.cmds.judge.ErrorJudge;
import io.mycat.mycat2.cmds.judge.OkJudge;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 
 * 0x17 COM_STMT_EXECUTE 执行预处理语句 此语句执行完毕后，可能会释放连接
 * 
 * @since 2017年8月23日 下午11:09:49
 * @version 0.0.1
 * @author liujun
 */
public class ComStmtExecuteHeaderHandlerAdapter implements CommandHandlerAdapter {

	/**
	 * 首包处理的实例对象
	 */
	public static final ComStmtExecuteHeaderHandlerAdapter INSTANCE = new ComStmtExecuteHeaderHandlerAdapter();

	@Override
	public boolean procss(MySQLSession session) throws IOException {

		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;

		ProxyBuffer curBuffer = session.proxyBuffer;

		// 进行首次的报文解析
		CurrPacketType pkgTypeEnum = session.resolveMySQLPackage(curBuffer, curMSQLPackgInf, true);

		// 首包，必须为全包进行解析，否则再读取一次，进行操作
		if (null != pkgTypeEnum && CurrPacketType.Full == pkgTypeEnum) {
			// 如果当前为错误包，则进交给错误包处理
			if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
				boolean runFlag = ErrorJudge.INSTANCE.judge(session);

				if (runFlag) {
					return true;
				}

				return false;
			}

			// 进行当前执行的预处理的语句报文，如果为Ok则表示可以释放连接,进行正常的判断
			else if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
				boolean runFlag = OkJudge.INSTANCE.judge(session);

				if (runFlag) {
					return true;
				}

				return false;
			}
		}

		// 其他包则交给stmt结果来处理来处理
		session.getMycatSession().commandHandler = ComStmtPrepareHandlerAdapter.INSTANCE;
		return true;

	}

}
