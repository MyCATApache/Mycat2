package io.mycat.mycat2.cmds;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;

/**
 * 进行命令处理的接口
 * 
 * @since 2017年8月15日 下午6:10:39
 * @version 0.0.1
 * @author liujun
 */
public interface SQLComandProcessInf {

	/**
	 * 进行命令处理的接口
	 * 
	 * @param session
	 *            会话信息
	 */
	public void commandProc(MySQLSession session) throws IOException;

}
