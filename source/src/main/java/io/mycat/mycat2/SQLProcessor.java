package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * 处理SQL命令，可以包括子命令
 * 
 * @author wuzhihui
 *
 */
public interface SQLProcessor {
	public enum HandleAbility {
		FullPkgParsed, CustomParse
	}

	/**
	 * 处理前端发来的报文
	 * 
	 * @param session
	 * 
	 */
	public void handFrontPackage(MySQLSession session) throws IOException;

	public void handBackendPackage(MySQLSession session) throws IOException;

	public void onFrontClosed(MySQLSession userSession, boolean normal);

	public void onBackendClosed(MySQLSession userSession, boolean normal);

}
