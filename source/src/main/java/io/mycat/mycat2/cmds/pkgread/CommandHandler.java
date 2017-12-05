package io.mycat.mycat2.cmds.pkgread;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;

/**
 * 进行后端的后端的命令验证操作
 * 
 * @since 2017年8月23日 下午11:08:26
 * @version 0.0.1
 * @author liujun
 */
public interface CommandHandler {

	/**
	 * 进行包操作的接口
	 * 
	 * @param session
	 *            后端会话信息
	 * @return true 继续处理，false退出处理
	 * @throws IOException
	 */
	public boolean procss(MySQLSession session) throws IOException;

}
