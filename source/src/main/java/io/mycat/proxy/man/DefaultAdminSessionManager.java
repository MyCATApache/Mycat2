package io.mycat.proxy.man;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;

import io.mycat.mycat2.tasks.AsynTaskCallBack;
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
	public void createSession(Object keyAttachement, BufferPool bufPool, Selector nioSelector, SocketChannel channel, AsynTaskCallBack<AdminSession> callBack) throws IOException {
		if (logger.isInfoEnabled()) {
			if (channel.isConnected()) {
				throw new RuntimeException("AdminSession is not connected " + channel);
			}
		}
			AdminSession session = new AdminSession(bufPool, nioSelector, channel);
			session.setCurNIOHandler(DefaultAdminSessionHandler.INSTANCE);
			String clusterNodeId = (String) keyAttachement;
			session.setNodeId(clusterNodeId);
			// session.setCurProxyHandler(proxyHandler);

			logger.info(" connected to cluster port  ." + channel + "create session " + session);
			session.setSessionManager(this);
			allSessions.add(session);
			if (callBack!=null){
				callBack.finished(session,this,true,null);
			}
	}

	@Override
	public Collection<AdminSession> getAllSessions() {
		return this.allSessions;
	}
	public void removeSession(AdminSession session) {
		this.allSessions.remove(session);

	}

	@Override
	public NIOHandler<AdminSession> getDefaultSessionHandler() {
		return DefaultAdminSessionHandler.INSTANCE;
	}



	@Override
	public int curSessionCount() {
		// TODO Auto-generated method stub
		return allSessions.size();
	}
}
