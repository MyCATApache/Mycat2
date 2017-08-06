package io.mycat.mycat2;

import java.io.IOException;

/**
 * 处理SQL命令，可以包括子命令
 * 
 * @author wuzhihui
 *
 */
public interface SQLProcessor {
	/**
	 * 处理前端发来的报文
	 * 
	 * @param session
	 * @return 是否要修改NIO
	 *         Key，如果读写感兴趣的事件发生改变，则需要返回TRUE，原先为读状态，当前需要继续读数据，
	 *         则不需要改变，返回FALSE即可触发第二次读事件
	 */
	public boolean handFrontPackage(MySQLSession session) throws IOException;

	public boolean handBackendPackage(MySQLSession session) throws IOException;

}
