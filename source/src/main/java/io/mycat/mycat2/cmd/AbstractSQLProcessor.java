package io.mycat.mycat2.cmd;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.SQLProcessor;
import io.mycat.proxy.ProxyBuffer;

public abstract class AbstractSQLProcessor implements SQLProcessor {
	private static Logger logger = LoggerFactory.getLogger(AbstractSQLProcessor.class);

	public boolean readPackage(MySQLSession session, boolean readFront) throws IOException {
		ProxyBuffer buffer = session.backendBuffer;
		SocketChannel channel = session.frontChannel;
		if (!readFront) {
			buffer = session.frontBuffer;
			channel = session.backendChannel;
		}
		int readed = session.readFromChannel(buffer, channel);
		logger.debug("readed {} bytes ", readed);
		if (readed == -1) {
			session.closeSocket(channel, true, "read EOF.");
			return false;
		} else if (readed == 0) {
			logger.info("read 0 bytes ,try compact buffer ,session Id :" + session.sessionId);
			buffer.compact(true);
			// todo curMSQLPackgInf
			// 也许要对应的改变位置,如果curMSQLPackgInf是跨Package的，则可能无需改变信息
			// curPackInf.
			return false;
		}
		buffer.updateReadLimit();
		return true;
	}

	@Override
	public void onFrontClosed(MySQLSession session, boolean normal) {
		session.lazyCloseSession();

	}

	@Override
	public void onBackendClosed(MySQLSession session, boolean normal) {
		session.lazyCloseSession();

	}
}
