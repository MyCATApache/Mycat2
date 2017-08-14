package io.mycat.mycat2.tasks;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.UserProxySession;
import io.mycat.proxy.UserProxySession.NetOptMode;

/**
 * 默认抽象类
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractBackendIOTask implements BackendIOTask {

	protected AsynTaskCallBack callBack;
	protected final MySQLSession session;
	private NetOptMode prevNetMode;
	private NIOHandler<MySQLSession> prevProxyHandler;
	protected ErrorPacket errPkg;

	public AbstractBackendIOTask(MySQLSession session) {
		prevNetMode = session.netOptMode;
		session.netOptMode = UserProxySession.NetOptMode.BackendRW;
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
		session.netOptMode = prevNetMode;
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
