package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.console.SessionKeyEnum;

/**
 * 进行error包的透传结束判断处理
 * 
 * @since 2017年8月19日 上午12:08:27
 * @version 0.0.1
 * @author liujun
 */
public class ErrorJudge implements DirectTransJudge {

	/**
	 * error包判断实例
	 */
	public static final ErrorJudge INSTANCE = new ErrorJudge();

	@Override
	public boolean judge(MySQLSession session) {
		// 进行当前
		// 首先检查是否处于事务中，如果非事务中，将结识连接结束
		if (!session.getSessionAttrMap().containsKey(SessionKeyEnum.SESSION_KEY_TRANSACTION_FLAG.getKey())) {
			// 标识当前处于闲置中,
			session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), true);
		}

		return false;
	}

}
