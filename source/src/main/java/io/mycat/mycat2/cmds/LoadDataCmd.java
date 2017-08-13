package io.mycat.mycat2.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLCommand;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.proxy.ProxyBuffer;

/**
 * 进行load data的命令数据透传
 * 
 * @author wuzhihui
 *
 */
public class LoadDataCmd implements SQLCommand {

	/**
	 * 结束flag标识
	 */
	private byte[] overFlag = new byte[4];

	@Override
	public boolean procssSQL(MySQLSession session, boolean backresReceived) throws IOException {

		ProxyBuffer curBuffer = session.backendBuffer;
		SocketChannel curChannel = session.backendChannel;
		if (backresReceived) {// 收到后端发来的报文

			curBuffer = session.frontBuffer;
			curChannel = session.frontChannel;
		}

		// 获取当前buffer的最后
		ByteBuffer buffer = curBuffer.getBuffer();

		int opts = curBuffer.readState.optLimit;
		buffer.position(opts - 4);
		buffer.get(overFlag, 0, 4);
		buffer.position(opts);

		session.writeToChannel(curBuffer, curChannel);

		// 结束进才进行状态的切换!
		if (checkOver()) {
			session.modifySelectKey();
			return true;
		}
		
		curBuffer.reset();

		return false;
	}

	private boolean checkOver() {
		for (int i = 0; i < overFlag.length - 1; i++) {
			if (overFlag[i] != 0) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void clearResouces(boolean sessionCLosed) {

	}

}
