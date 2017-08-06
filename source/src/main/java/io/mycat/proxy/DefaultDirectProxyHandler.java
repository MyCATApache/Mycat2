
package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.proxy.UserSession.NetOptMode;

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
				boolean bufferWriteFinished = this.socketWriteFromBuf(false, userSession, userSession.backendChannel,
						userSession.backendBuffer);
				if (bufferWriteFinished) {
					this.onBackendWriteFinished(userSession);
				}
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
					boolean writeFinished = onFrontWriteEvent(userSession);
					if (writeFinished) {
						afterSocketWrite(userSession, userSession.frontBuffer);
					}
				} else {
					boolean writeFinished = onBackendWriteEvent(userSession);
					if (writeFinished) {
						afterSocketWrite(userSession, userSession.backendBuffer);
					}
				}
			}
		} catch (final Exception exception) {
			onSocketException(userSession, exception);
		}

	}

	public boolean onFrontWriteEvent(T userSession) throws IOException {
		return this.socketWriteFromBuf(true, userSession, userSession.frontChannel, userSession.frontBuffer);

	}

	public boolean onBackendWriteEvent(T userSession) throws IOException {
		return this.socketWriteFromBuf(false, userSession, userSession.backendChannel, userSession.backendBuffer);
	}

	protected boolean socketWriteFromBuf(boolean frontChannel, T userSession, SocketChannel channel,
			ProxyBuffer proxyBuffer) throws IOException {

		boolean writeFinished = proxyBuffer.readToChannel(channel);

		return writeFinished;
	}

	protected void afterSocketWrite(T userSession, ProxyBuffer proxyBuffer) throws IOException {

		// 表明NIO线程可以读取对端Socket中发来的数据并且写入此Buffer中，等待下一次传输
		proxyBuffer.flip();
		modifySelectKey(userSession);
	}

	public void closeSocket(T userSession, SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = ((normal) ? " normal close " : "abnormal close ")
				+ ((channel == userSession.frontChannel) ? " front socket. " : " backend socket. ");
		logger.info(logInf + userSession.sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
		if (channel == userSession.frontChannel) {
			onFrontSocketClosed(userSession, normal);
			userSession.frontChannel = null;
		} else if (channel == userSession.frontChannel) {
			onBackendSocketClosed(userSession, normal);
			userSession.backendChannel = null;
		}

	}

	/**
	 * 根据网络模式确定是否修改前端连接与后端连接的NIO感兴趣事件
	 * 
	 * @param userSession
	 * @throws ClosedChannelException
	 */
	public void modifySelectKey(T userSession) throws ClosedChannelException {
		boolean frontKeyNeedUpdate = false;
		boolean backKeyNeedUpdate = false;
		switch (userSession.netOptMode) {
		case FrontRW: {
			frontKeyNeedUpdate = true;
			backKeyNeedUpdate = false;
			break;
		}
		case BackendRW: {
			frontKeyNeedUpdate = false;
			backKeyNeedUpdate = true;
			break;
		}
		case DirectTrans: {
			frontKeyNeedUpdate = true;
			backKeyNeedUpdate = true;
			break;
		}
		}
		if (frontKeyNeedUpdate && userSession.frontKey != null && userSession.frontKey.isValid()) {
			int clientOps = 0;
			if (userSession.backendBuffer.isInWriting())
				clientOps |= SelectionKey.OP_READ;
			if (userSession.frontBuffer.isInWriting() == false)
				clientOps |= SelectionKey.OP_WRITE;
			userSession.frontKey.interestOps(clientOps);
		}
		if (backKeyNeedUpdate && userSession.backendKey != null && userSession.backendKey.isValid()) {
			int serverOps = 0;
			if (userSession.frontBuffer.isInWriting())
				serverOps |= SelectionKey.OP_READ;
			if (userSession.backendBuffer.isInWriting() == false)
				serverOps |= SelectionKey.OP_WRITE;
			userSession.backendKey.interestOps(serverOps);
		}
	}

	/**
	 * 前端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onFrontSocketClosed(T userSession, boolean normal) {
		lazyCloseSession(userSession);
	}

	/**
	 * 后端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onBackendSocketClosed(T userSession, boolean normal) {
		lazyCloseSession(userSession);
	}

	@SuppressWarnings("rawtypes")
	public void lazyCloseSession(final T userSession) {
		if (userSession.isClosed()) {
			return;
		}

		ProxyRuntime.INSTANCE.addDelayedNIOJob(() -> {
			if (!userSession.isClosed()) {
				userSession.close("front closed");
			}
		}, 10, (ProxyReactorThread) Thread.currentThread());
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

		int readed = userSession.backendBuffer.writeFromChannel(userSession.frontChannel);
		if (readed == -1) {
			this.closeSocket(userSession, userSession.frontChannel, true, "read EOF.");
		} else if (readed > 0) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			userSession.backendBuffer.flip();
			if (userSession.backendBuffer.isInReading()) {
				modifySelectKey(userSession);
			}
		}
	}

	public void onBackendReaded(T userSession) throws IOException {
		int readed = userSession.frontBuffer.writeFromChannel(userSession.backendChannel);
		if (readed == -1) {
			this.closeSocket(userSession, userSession.backendChannel, true, "read EOF.");
		} else if (readed > 0) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			userSession.frontBuffer.flip();
			if (userSession.frontBuffer.isInReading()) {
				modifySelectKey(userSession);
			}
		}

	}

	public void onFrontWriteFinished(T session) throws IOException {
		afterSocketWrite(session, session.frontBuffer);

	}

	public void onBackendWriteFinished(T session) throws IOException {
		afterSocketWrite(session, session.backendBuffer);

	}
}
