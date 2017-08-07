
package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认透传的Proxy Handler
 * 
 * @author wuzhihui
 *
 */
public class DefaultDirectProxyHandler<T extends UserSession> implements NIOProxyHandler<T> {

	protected static Logger logger = LoggerFactory.getLogger(DefaultDirectProxyHandler.class);

	public void onFrontConnected(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel)
			throws IOException {
		logger.info("front connected  ." + frontChannel);

		UserSession session = new UserSession(bufPool, nioSelector, frontChannel);
		session.bufPool = bufPool;
		session.nioSelector = nioSelector;
		session.frontChannel = frontChannel;
		InetSocketAddress clientAddr = (InetSocketAddress) frontChannel.getRemoteAddress();
		session.frontAddr = clientAddr.getHostString() + ":" + clientAddr.getPort();
		SelectionKey socketKey = frontChannel.register(nioSelector, SelectionKey.OP_READ, session);
		session.frontKey = socketKey;

		// todo ,from config
		// 尝试连接Server 端口
		String serverIP = "localhost";
		int serverPort = 3306;
		InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
		session.backendChannel = SocketChannel.open();
		session.backendChannel.configureBlocking(false);
		session.backendChannel.connect(serverAddress);
		SelectionKey selectKey = session.backendChannel.register(session.nioSelector, SelectionKey.OP_CONNECT, session);
		session.backendKey = selectKey;
		logger.info("Connecting to server " + serverIP + ":" + serverPort);

	}

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

	public void handIO(T userSession, SelectionKey selectionKey) {
		try {
			boolean isFront = (selectionKey.channel() == userSession.frontChannel) ? true : false;
			if (selectionKey.isReadable()) {
				if (isFront) {
					onFrontReaded(userSession);
				} else {
					onBackendReaded(userSession);
				}
			}
			if (selectionKey.isValid() && selectionKey.isWritable()) {
				if (isFront) {
					userSession.writeToChannel(userSession.frontBuffer, userSession.frontChannel);
				} else {
					userSession.writeToChannel(userSession.backendBuffer, userSession.backendChannel);

				}
			}
		} catch (final Exception exception) {
			onSocketException(userSession, exception);
		}

	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onFrontSocketClosed(T userSession, boolean normal) {
		userSession.lazyCloseSession();
	}

	/**
	 * 后端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onBackendSocketClosed(T userSession, boolean normal) {
		userSession.lazyCloseSession();
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

	public void onFrontReaded(T userSession) throws IOException {

		int readed = userSession.readFromChannel(userSession.backendBuffer,userSession.frontChannel);
		if (readed == -1) {
			userSession.closeSocket(userSession.frontChannel, true, "read EOF.");
		} else if (readed > 0) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			userSession.backendBuffer.flip();
			userSession.modifySelectKey();
		}
	}

	public void onBackendReaded(T userSession) throws IOException {
		int readed = userSession.readFromChannel(userSession.frontBuffer,userSession.backendChannel);
		if (readed == -1) {
			userSession.closeSocket(userSession.backendChannel, true, "read EOF.");
		} else if (readed > 0) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			userSession.frontBuffer.flip();
			userSession.modifySelectKey();
		}

	}
}
