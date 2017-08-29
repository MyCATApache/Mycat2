package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mycat2.net.DefaultMycatSessionHandler;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行ok包的透传结束判断处理
 * 
 * @since 2017年8月19日 上午12:08:27
 * @version 0.0.1
 * @author liujun
 */
public class OkJudge implements DirectTransJudge {

	/**
	 * ok包判断实例
	 */
	public static final OkJudge INSTANCE = new OkJudge();

	@Override
	public boolean judge(MySQLSession session) {
		ProxyBuffer curBuffer = session.proxyBuffer;
		// 进行当前
		curBuffer.readIndex = session.curMSQLPackgInf.startPos;
		// 进行当前ok包的读取
		OKPacket okpkg = new OKPacket();
		okpkg.read(curBuffer);

		boolean multQuery = ServerStatusEnum.StatusCheck(okpkg.serverStatus, ServerStatusEnum.MULT_QUERY);
		boolean multResult = ServerStatusEnum.StatusCheck(okpkg.serverStatus, ServerStatusEnum.MORE_RESULTS);

		if (multQuery || multResult) {
			// 标识当前处于使用中,不能结束
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
			return true;
		}

		// 事务状态的检查
		boolean trans = ServerStatusEnum.StatusCheck(okpkg.serverStatus, ServerStatusEnum.IN_TRANSACTION);

		// 如果当前事务状态被设置，连接标识为不能结束
		if (trans) {
			// 标识当前处于使用中，不能结束,
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
			// 如果发现事务标识，则标识当前处于会话中
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_TRANSACTION_FLAG.getKey(), true);
		}
		// 当连接使用完毕，则标识为可以结束
		else {
			// 标识当前处于闲置中,
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), true);
			// 当发现完毕后，将标识移除
			session.getSessionAttrMap().remove(SessionKeyEnum.SESSION_KEY_TRANSACTION_FLAG.getKey());
		}

		return false;

	}

}
