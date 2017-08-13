package io.mycat.proxy.man;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.FrontIOHandler;
import io.mycat.proxy.ProxyRuntime;

/**
 * 负责处理AdminSession的命令
 * 
 * @author wuzhihui
 *
 */
public class DefaultAdminSessionHandler implements FrontIOHandler<AdminSession> {
	private static Logger logger = LoggerFactory.getLogger(DefaultAdminSessionHandler.class);

	@Override
	public void onFrontRead(final AdminSession session) throws IOException {
		boolean readed = session.readSocket();
		byte pkgType = -1;
		// 没有读到完整报文
		if (readed == false || (pkgType = session.receivedPacket()) == -1) {
			return;
		}
		if (pkgType == ManagePacket.PKG_FAILED || pkgType == ManagePacket.PKG_SUCCESS) {
			session.curAdminCommand.handlerPkg(session);
		} else {
			session.curAdminCommand = ProxyRuntime.INSTANCE.getAdminCmdResolver().resolveCommand(pkgType);
			session.curAdminCommand.handlerPkg(session);
		}

	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onFrontSocketClosed(AdminSession userSession, boolean normal) {
		logger.info("front socket closed ");
		userSession.cluster().onClusterNodeDown(userSession.getNodeId());

	}

	@Override
	public void onFrontWrite(AdminSession session) throws IOException {
		session.writeToChannel(session.frontBuffer, session.frontChannel);

	}

}
