package io.mycat.mycat2.net;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.proxy.DefaultDirectProxyHandler;
import io.mycat.proxy.ProxyBuffer;

/**
 * 负责处理通用的SQL命令，默认情况下透传
 * 
 * @author wuzhihui
 *
 */
public class DefaultSQLHandler extends DefaultDirectProxyHandler<MySQLSession> {
	private static Logger logger = LoggerFactory.getLogger(DefaultSQLHandler.class);
	public static DefaultSQLHandler INSTANCE = new DefaultSQLHandler();
	// private ArrayList<Runnable> pendingJob;

	@Override
	public void onFrontRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false || session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf) == false) {
			return;
		}
		if (backendBuffer.readState.hasRemain()) {
			logger.warn("front read half package ");
		}
		session.writeToChannel(backendBuffer, session.backendChannel);
		return;

	}



}
