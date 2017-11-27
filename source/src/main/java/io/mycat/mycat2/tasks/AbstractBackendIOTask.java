package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;

/**
 * 默认抽象类
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractBackendIOTask<T extends AbstractMySQLSession> implements NIOHandler<T> {

	protected AsynTaskCallBack<T> callBack;
	protected T session;
	protected ProxyBuffer prevProxyBuffer;
	protected ErrorPacket errPkg;
	protected boolean useNewBuffer = false;

	public AbstractBackendIOTask(T session, boolean useNewBuffer) {
		setSession(session, useNewBuffer);
	}

	public AbstractBackendIOTask() {
		this(null, false);
	}
	public void setSession(T session, boolean useNewBuffer) {
		setSession( session, useNewBuffer, true);
	}
	public void setSession(T session, boolean useNewBuffer, boolean useNewCurHandler) {
		this.useNewBuffer = useNewBuffer;
		if (useNewBuffer) {
			prevProxyBuffer = session.proxyBuffer;
			session.proxyBuffer = session.allocNewProxyBuffer();
			session.setCurBufOwner(true);
		}
		if (session != null && useNewCurHandler) {
			this.session = session;
			session.setCurNIOHandler(this);
		}
	}

	protected void finished(boolean success) throws IOException {
		if (useNewBuffer) {
			revertPreBuffer();
		}
		onFinished(success);
		callBack.finished(session, this, success, this.errPkg);
	}

	protected void revertPreBuffer() {
		session.recycleAllocedBuffer(session.proxyBuffer);
		session.proxyBuffer = this.prevProxyBuffer;
	}

	protected void onFinished(boolean success) {

	}

	public void onConnect(SelectionKey theKey, MySQLSession userSession, boolean success, String msg)
			throws IOException {

	}

	public void setCallback(AsynTaskCallBack<T> callBack) {
		this.callBack = callBack;

	}
	public AsynTaskCallBack<T>	getCallback() {
		return this.callBack ;

	}
	@Override
	public void onSocketClosed(T userSession, boolean normal) {
	}

	@Override
	public void onSocketWrite(T session) throws IOException {
		session.writeToChannel();

	}

	@Override
	public void onWriteFinished(T s) throws IOException {
		s.proxyBuffer.reset();
		s.change2ReadOpts();

	}

}
