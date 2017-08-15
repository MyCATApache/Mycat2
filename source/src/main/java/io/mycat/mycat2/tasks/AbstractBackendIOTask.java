package io.mycat.mycat2.tasks;

import java.io.IOException;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.UserProxySession;

/**
 * 默认抽象类
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractBackendIOTask implements BackendIOTask {

	protected AsynTaskCallBack callBack;
	protected final MySQLSession session;
	protected ProxyBuffer prevProxyBuffer;
	protected NIOHandler<MySQLSession> prevProxyHandler;
	
	protected ErrorPacket errPkg;

	public AbstractBackendIOTask(MySQLSession session) {
		prevProxyBuffer=session.frontBuffer;
		session.frontBuffer=session.allocNewProxyBuffer();
		this.session = session;
		prevProxyHandler = session.getCurNIOHandler();
	}

	protected void finished(boolean success) throws IOException {
		sessionRecover();
		onFinished(success);

		callBack.finished(session, this, success, this.errPkg);
	}

	protected void sessionRecover() {
		// 恢复Session原来的状态
		session.recycleAllocedBuffer(session.frontBuffer);
		session.frontBuffer=this.prevProxyBuffer;
		session.setCurNIOHandler(prevProxyHandler);
	}

	protected void onFinished(boolean success) {

	}

	@Override
	public void onBackendConnect(MySQLSession userSession, boolean success, String msg) throws IOException {

	}

	@Override
	public void onBackendWrite(MySQLSession session) throws IOException {

	}

	@Override
	public void setCallback(AsynTaskCallBack callBack) {
		this.callBack = callBack;

	}

	

}
