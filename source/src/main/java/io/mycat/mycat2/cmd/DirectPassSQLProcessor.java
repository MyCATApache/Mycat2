package io.mycat.mycat2.cmd;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.proxy.ProxyBuffer;

public class DirectPassSQLProcessor extends AbstractSQLProcessor {
	private static Logger logger = LoggerFactory.getLogger(DirectPassSQLProcessor.class);
	public static DirectPassSQLProcessor INSTANCE = new DirectPassSQLProcessor();
	// private ArrayList<Runnable> pendingJob;

	@Override
	public void handFrontPackage(MySQLSession session) throws IOException {
		boolean readed =readPackage(session,true);
		ProxyBuffer backendBuffer = session.backendBuffer;
		if (readed == false
				|| session.resolveMySQLPackage(backendBuffer, session.curFrontMSQLPackgInf) == false) {
			return;
		}
		if (backendBuffer.readState.hasRemain()) {
			logger.warn("front read half package ");
		}
		session.writeToChannel(backendBuffer, session.backendChannel);
		return;

	}

	@Override
	public void handBackendPackage(MySQLSession session) throws IOException {
		boolean readed = readPackage(session,false);
		ProxyBuffer frontBuffer = session.frontBuffer;
		if (readed == false
				|| session.resolveMySQLPackage(frontBuffer, session.curBackendMSQLPackgInf) == false) {
			return;
		}
		while (frontBuffer.readState.hasRemain()) {
			boolean resolved = session.resolveMySQLPackage(frontBuffer, session.curBackendMSQLPackgInf);
			if (!resolved) {
				logger.warn("has half pakcage ");
				
				break;
			}
		}
		session.writeToChannel(frontBuffer, session.frontChannel);
		return;
	}

}
