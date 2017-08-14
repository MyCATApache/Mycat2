package io.mycat.mycat2.net;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.proxy.DefaultDirectProxyHandler;
import io.mycat.proxy.ProxyBuffer;

/**
 * 代理MySQL的ProxyHandler，可以用来研究报文流程，结构，MySQL报文发送特点， 也可以用来抓取SQL执行的性能数据，用来做智能分析
 * 
 * @author wuzhihui
 *
 */
public class DefaultMySQLStudySessionHandler extends DefaultDirectProxyHandler<MySQLSession> {

	public static final DefaultMySQLStudySessionHandler INSTANCE=new DefaultMySQLStudySessionHandler();
	@Override
	public void onFrontRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(true);
		ProxyBuffer peerBuf = session.backendBuffer;
		SocketChannel peerChannel = session.backendChannel;
		MySQLPackageInf curPkgInf = session.curFrontMSQLPackgInf;
		if (readed == false || session.resolveMySQLPackage(peerBuf, curPkgInf,true) == false) {
			return;
		}

		if (peerBuf.readState.hasRemain()) {
			logger.warn("front received multi packages !!!!! ");
			processAllRemainPkg(session, peerBuf, curPkgInf);

		}
		// 透传给对端
		peerBuf.flip();
		session.writeToChannel(peerBuf, peerChannel);
		return;

	}

	private void processAllRemainPkg(MySQLSession session, ProxyBuffer theBuf, MySQLPackageInf curPkgInf)
			throws IOException {
		int pkgIndex = 2;
		while (theBuf.readState.hasRemain() && session.resolveMySQLPackage(theBuf, curPkgInf,true) != false) {
			logger.info(" parsed No." + pkgIndex + " package ,type " + curPkgInf.pkgType + " len " + curPkgInf.pkgLength);
			pkgIndex++;
		}
		if (theBuf.readState.hasRemain()) {
			logger.warn("has half package remains ");

		}
	}

	@Override
	public void onBackendRead(MySQLSession session) throws IOException {
		boolean readed = session.readSocket(false);
		ProxyBuffer peerBuf = session.frontBuffer;
		SocketChannel peerChannel = session.frontChannel;
		MySQLPackageInf curPkgInf = session.curBackendMSQLPackgInf;
		if (readed == false || session.resolveMySQLPackage(peerBuf, curPkgInf,true) == false) {
			return;
		}

		if (peerBuf.readState.hasRemain()) {
			logger.warn("backend received multi packages !!!!! ");
			processAllRemainPkg(session, peerBuf, curPkgInf);

		}
		// 透传给对端
		peerBuf.flip();
		session.writeToChannel(peerBuf, peerChannel);
		return;
	}

}
