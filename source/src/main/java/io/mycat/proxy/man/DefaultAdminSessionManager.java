package io.mycat.proxy.man;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.Session;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.man.packet.NodeRegInfoPacket;

/**
 * 默认的管理会话的SessionManager
 * 
 * @author wuzhihui
 *
 */
public class DefaultAdminSessionManager implements SessionManager<AdminSession> {
	protected static Logger logger = LoggerFactory.getLogger(DefaultAdminSessionManager.class);
	private ArrayList<AdminSession> allSessions = new ArrayList<>();

	@Override
	public AdminSession createSession(Object keyAttachement ,BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel,
			boolean isAcceptedCon) throws IOException {

		AdminSession session = new AdminSession(bufPool, nioSelector, frontChannel);
		session.setCurNIOHandler(DefaultAdminSessionHandler.INSTANCE);
		String clusterNodeId = (String) keyAttachement;
		session.setNodeId(clusterNodeId);
		// session.setCurProxyHandler(proxyHandler);
		if (isAcceptedCon) {// 客户端连接上来，所以发送信息给客户端
			NodeRegInfoPacket nodeRegInf = new NodeRegInfoPacket(session.cluster().getMyNodeId(),
					session.cluster().getClusterState(), session.cluster().getLastClusterStateTime(),
					session.cluster().getMyLeaderId(), ProxyRuntime.INSTANCE.getStartTime());
			session.answerClientNow(nodeRegInf);
		}
		logger.info(" connected to cluster port  ." + frontChannel + "create session " + session);
		session.setSessionManager(this);
		allSessions.add(session);
		return session;
	}

	@Override
	public Collection<AdminSession> getAllSessions() {
		return this.allSessions;
	}
	public void removeSession(Session session) {
		this.allSessions.remove(session);

	}

	@Override
	public NIOHandler getDefaultSessionHandler() {
		return DefaultAdminSessionHandler.INSTANCE;
	}
}
