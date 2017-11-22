package io.mycat.mycat2.cmds.pkgread;

import java.io.IOException;

import io.mycat.mycat2.AbstractMySQLSession.CurrPacketType;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 
 * 0x1A COM_STMT_RESET 清除预处理语句参数缓存
 * 
 * 因为处于预编译执行与结果响应中间的报文，故不能释放连接
 * 
 * @since 2017年8月23日 下午11:09:49
 * @version 0.0.1
 * @author liujun
 */
public class ComStmtResetHandlerAdapter implements CommandHandlerAdapter {

	/**
	 * 首包处理的实例对象
	 */
	public static final ComStmtResetHandlerAdapter INSTANCE = new ComStmtResetHandlerAdapter();

	@Override
	public boolean procss(MySQLSession session) throws IOException {

		MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;

		ProxyBuffer curBuffer = session.proxyBuffer;

		// 进行首次的报文解析
		CurrPacketType pkgTypeEnum = session.resolveMySQLPackage(curBuffer, curMSQLPackgInf, true);

		// 标识当前传输未结束
		MycatSession mycatSession = session.getMycatSession();

		// 首包，必须为全包进行解析，否则再读取一次，进行操作
		if (null != pkgTypeEnum && CurrPacketType.Full == pkgTypeEnum) {
			// 如果当前为错误包，则进交给错误包处理
			if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
				// 标识连接当前非闲置
				session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
				// 标识当后端向前端响应已经结束
				mycatSession.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey());
				return false;
			}
			// 如果是ok报文
			else if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
				// 标识连接当前非闲置
				session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
				// 标识当后端向前端响应已经结束
				mycatSession.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSFER_OVER_FLAG.getKey());
				return false;
			}
		}

		return false;

	}

}
