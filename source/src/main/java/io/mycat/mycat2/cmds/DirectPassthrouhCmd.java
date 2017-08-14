package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.proxy.ProxyBuffer;

/**
 * 直接透传命令报文
 * 
 * @author wuzhihui
 *
 */
public class DirectPassthrouhCmd implements SQLCommand {

	public static final DirectPassthrouhCmd INSTANCE=new DirectPassthrouhCmd();
	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException {

		ProxyBuffer curBuffer = session.backendBuffer;
		SocketChannel curChannel = session.backendChannel;
		if (backresReceived) {// 收到后端发来的报文

			curBuffer = session.frontBuffer;
			curChannel = session.frontChannel;
		}

		// 直接透传报文
		curBuffer.flip();
		session.writeToChannel(curBuffer, curChannel);
		session.modifySelectKey();
		return false;
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {
		// nothint to do

	}

}
