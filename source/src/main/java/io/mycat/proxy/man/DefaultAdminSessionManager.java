package io.mycat.proxy.man;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.BufferPool;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.Session;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.man.packet.NodeRegInfoPacket;

/**
 * 默认的管理会话的SessionManager
 * 
 * @author wuzhihui
 *
 */
public class DefaultAdminSessionManager implements SessionManager<Session> {
	protected static Logger logger = LoggerFactory.getLogger(DefaultAdminSessionManager.class);

	@Override
	public Session createSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel,
			boolean isAcceptedCon) throws IOException {

		AdminSession session = new AdminSession(bufPool, nioSelector, frontChannel);
		// session.setCurProxyHandler(proxyHandler);
		if (isAcceptedCon) {// 客户端连接上来，所以发送信息给客户端
			NodeRegInfoPacket nodeRegInf = new NodeRegInfoPacket(session.cluster().getMyNode().id,
					ProxyRuntime.INSTANCE.getStartTime());
			session.answerClientNow(nodeRegInf);
		}
		// 向客户端发送自身信息

		// TODO setproxy
		logger.info(" connected to admin port  ." + frontChannel + "create session " + session);

		return session;
	}

}
