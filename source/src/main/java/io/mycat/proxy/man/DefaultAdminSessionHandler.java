package io.mycat.proxy.man;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.ConnectIOHandler;
import io.mycat.proxy.FrontIOHandler;
import io.mycat.proxy.ProxyRuntime;

/**
 * 负责处理AdminSession的命令
 * 
 * @author wuzhihui
 *
 */
public class DefaultAdminSessionHandler implements FrontIOHandler<AdminSession>, ConnectIOHandler<AdminSession> {
	private static Logger logger = LoggerFactory.getLogger(DefaultAdminSessionHandler.class);
    public static final DefaultAdminSessionHandler INSTANCE=new DefaultAdminSessionHandler();
	@Override
	public void onFrontRead(final AdminSession session) throws IOException {
		boolean readed = session.readSocket();
		byte pkgType = -1;
		// 没有读到完整报文
		if (readed == false || (pkgType = session.receivedPacket()) == -1) {
			return;
		}
		if (pkgType == ManagePacket.PKG_FAILED || pkgType == ManagePacket.PKG_SUCCESS) {
			session.curAdminCommand.handlerPkg(session,pkgType);
		} else {
			session.curAdminCommand = ProxyRuntime.INSTANCE.getAdminCmdResolver().resolveCommand(pkgType);
			session.curAdminCommand.handlerPkg(session,pkgType);
		}

	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 * @throws IOException 
	 */
	public void onFrontSocketClosed(AdminSession userSession, boolean normal)  {
		logger.info("front socket closed ");
		userSession.cluster().onClusterNodeDown(userSession.getNodeId(),userSession);

	}

	@Override
	public void onFrontWrite(AdminSession session) throws IOException {
		session.writeToChannel(session.frontBuffer, session.frontChannel);

	}

	@Override
	public void onConnect(SelectionKey key ,AdminSession userSession, boolean success, String msg) throws IOException {
		logger.info(" socket connect " + ((success) ? " success " : " failed: " + msg));
		
	}

}
