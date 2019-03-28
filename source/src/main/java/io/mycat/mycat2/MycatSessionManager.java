package io.mycat.mycat2;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import io.mycat.mycat2.tasks.AsynTaskCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.net.MainMycatNIOHandler;
import io.mycat.mycat2.net.MySQLClientAuthHandler;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.SessionManager;
import io.mycat.proxy.buffer.BufferPool;

/**
 * Mycat 2.0 Session Manager
 * 
 * @author wuzhihui
 * @checked
 */
public class MycatSessionManager implements SessionManager<MycatSession> {
	protected static Logger logger = LoggerFactory.getLogger(MycatSessionManager.class);
	private LinkedList<MycatSession> allSessions = new LinkedList<>();


	/**
	 * jdbc客户端连接mycat第一个报文回调的函数,在此函数处理中,如果buffer过小导致无法一次写入完整的验证包,则会抛出异常停机
	 *  cjw
	 * @param
	 * @param bufPool
	 *            用来获取Buffer的Pool
	 * @param nioSelector
	 *            注册到对应的Selector上
	 * @param frontChannel
	 * @return
	 * @throws IOException
	 */
	public MycatSession createSessionForConnectedChannel(Object keyAttachement, BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException {
		if (logger.isInfoEnabled()) {
			if (!frontChannel.isConnected()){
				throw new RuntimeException("MySQL client is not connected "+frontChannel);
			}
		}
		MycatSession session = new MycatSession(bufPool, nioSelector, frontChannel,MySQLClientAuthHandler.INSTANCE);
		// 第一个IO处理器为Client Authorware
		session.setCurNIOHandler(MySQLClientAuthHandler.INSTANCE);
		// 默认为透传命令模式
		// session.curSQLCommand = DirectPassthrouhCmd.INSTANCE;
		// 向MySQL Client发送认证报文
		session.sendAuthPackge();
		session.setSessionManager(this);
		allSessions.add(session);
		return session;
	}

	@Override
	public Collection<MycatSession> getAllSessions() {
		return this.allSessions;
	}

	public void removeSession(MycatSession session) {
		this.allSessions.remove(session);

	}

	@Override
	public int curSessionCount() {
		return allSessions.size();
	}

	@Override
	public NIOHandler<MycatSession> getDefaultSessionHandler() {
		return MainMycatNIOHandler.INSTANCE;
	}

}
