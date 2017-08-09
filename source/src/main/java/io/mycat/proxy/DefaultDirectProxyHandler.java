
package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认透传的Proxy Handler
 * 
 * @author wuzhihui
 *
 */
public class DefaultDirectProxyHandler<T extends UserSession> implements FrontIOHandler<T> ,BackendIOHandler<T>{
	protected static Logger logger = LoggerFactory.getLogger(DefaultDirectProxyHandler.class);
	public void onBackendConnect(T userSession, boolean success, String msg) throws IOException {
		String logInfo = success ? " backend connect success " : "backend connect failed " + msg;
		logger.info(logInfo + " channel " + userSession.backendChannel);
		if (success) {
			InetSocketAddress serverRemoteAddr = (InetSocketAddress) userSession.backendChannel.getRemoteAddress();
			InetSocketAddress serverLocalAddr = (InetSocketAddress) userSession.backendChannel.getLocalAddress();
			userSession.backendAddr = "local port:" + serverLocalAddr.getPort() + ",remote "
					+ serverRemoteAddr.getHostString() + ":" + serverRemoteAddr.getPort();
			userSession.backendChannel.register(userSession.nioSelector, SelectionKey.OP_READ, userSession);
			// 如果发现前端有数据写入到后端的Buffer，就尝试转写到后端
			if (userSession.backendBuffer.isInReading()) {
				userSession.writeToChannel(userSession.backendBuffer, userSession.backendChannel);
			}
		} else {
			userSession.close("backend can't open:" + msg);
		}

	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onFrontSocketClosed(T userSession, boolean normal) {
		userSession.lazyCloseSession("front closed");
	}

	/**
	 * 后端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onBackendSocketClosed(T userSession, boolean normal) {
		userSession.lazyCloseSession("backend closed");
	}

	/**
	 * Socket IO读写过程中出现异常后的操作，通常是要关闭Session的
	 * 
	 * @param userSession
	 * @param exception
	 */
	protected void onSocketException(UserSession userSession, Exception exception) {
		if (exception instanceof IOException) {
			logger.warn("ProxyTransDataNIOHandler handle IO error " + userSession.sessionInfo() + " "
					+ exception.getMessage());

		} else {
			logger.warn("ProxyTransDataNIOHandler handle IO error " + userSession.sessionInfo(), exception);
		}
		userSession.close("exception:" + exception.getMessage());
	}

	public void onFrontRead(T userSession) throws IOException {

		int readed = userSession.readFromChannel(userSession.backendBuffer, userSession.frontChannel);
		if (readed == -1) {
			userSession.closeSocket(userSession.frontChannel, true, "read EOF.");
		} else if (readed > 0) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			userSession.backendBuffer.flip();
			userSession.modifySelectKey();
		}
	}

	public void onBackendRead(T userSession) throws IOException {
		int readed = userSession.readFromChannel(userSession.frontBuffer, userSession.backendChannel);
		if (readed == -1) {
			userSession.closeSocket(userSession.backendChannel, true, "read EOF.");
		} else if (readed > 0) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			userSession.frontBuffer.flip();
			userSession.modifySelectKey();
		}

	}

	@Override
	public void onFrontWrite(T session) throws IOException {
		session.writeToChannel(session.frontBuffer, session.frontChannel);

	}

	@Override
	public void onBackendWrite(T session) throws IOException {
		session.writeToChannel(session.backendBuffer, session.backendChannel);

	}
}
