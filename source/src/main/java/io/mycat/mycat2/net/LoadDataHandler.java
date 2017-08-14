package io.mycat.mycat2.net;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.DirectPassthrouhCmd;
import io.mycat.proxy.ProxyBuffer;

/**
 * 负责处理通用的SQL命令，默认情况下透传
 * 
 * @author wuzhihui
 *
 */
public class LoadDataHandler extends DefaultSQLHandler {
	private static Logger logger = LoggerFactory.getLogger(LoadDataHandler.class);
	public static LoadDataHandler INSTANCE = new LoadDataHandler();
	public static DirectPassthrouhCmd defaultSQLCmd = new DirectPassthrouhCmd();

	@Override
	public void onFrontRead(final MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false) {
			return;
		}
		if (session.curFrontMSQLPackgInf.endPos < backendBuffer.getReadOptState().optLimit) {
			logger.warn("front contains multi package ");
		}
		// 交给SQLComand去处理
		if (session.curSQLCommand.procssSQL(session, false)) {
			session.curSQLCommand.clearResouces(false);
			session.curSQLCommand = defaultSQLCmd;
			session.curProxyHandler = INSTANCE;
		}

	}

}
