package io.mycat.mycat2.cmds.judge;

import io.mycat.mycat2.MySQLSession;

/**
 * 接口传输判断接口
 * @since 2017年8月18日 下午11:41:46
 * @version 0.0.1
 * @author liujun
 */
public interface DirectTransJudge {
	
	/**
	 *  当满足判断的类型后，进行的判断执行
	 * @param session 后端会话对象信息 
	 * @return 是否需要进行一步的读取 true 需要进一步的读取 false不需要,一般查询的eof有可能会进一步的读取，其他都不需要
	 */
	public boolean judge(MySQLSession session);

}
