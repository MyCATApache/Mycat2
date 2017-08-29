package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行query包的透传结束判断处理
 * 
 * @since 2017年8月19日 上午12:08:27
 * @version 0.0.1
 * @author liujun
 */
public class EofJudge implements DirectTransJudge {

	/**
	 * eof包判断实例
	 */
	public static final EofJudge INSTANCE = new EofJudge();

	@Override
	public boolean judge(MySQLSession session) {

		ProxyBuffer curBuffer = session.proxyBuffer;
		// 进行当前
		curBuffer.readIndex = session.curMSQLPackgInf.startPos;
		// 进行当前ok包的读取
		EOFPacket eofPkg = new EOFPacket();
		eofPkg.read(curBuffer);

		boolean multQuery = ServerStatusEnum.StatusCheck(eofPkg.status, ServerStatusEnum.MULT_QUERY);
		boolean multResult = ServerStatusEnum.StatusCheck(eofPkg.status, ServerStatusEnum.MORE_RESULTS);

		if (multQuery || multResult) {
			// 标识当前处于使用中
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
			return true;
		}

		// 事务状态的检查
		boolean trans = ServerStatusEnum.StatusCheck(eofPkg.status, ServerStatusEnum.IN_TRANSACTION);

		// 检查当前是否需要需要进行下一次的数据读取

		// 如果当前事务状态被设置，连接标识为不能结束
		if (trans) {
			// 标识当前处于使用中
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
			// 标识当前处于事物中
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
