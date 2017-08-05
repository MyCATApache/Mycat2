
package io.mycat.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import io.mycat.proxy.ProxyBuffer.BufferState;

/**
 * Handler for client/server communications.
 */
public class DefaultDirectProxyHandler implements NIOProxyHandler {

	protected static Logger logger = Logger.getLogger(DefaultDirectProxyHandler.class);

	public void onFrontConnected(UserSession userSession) throws IOException {
		logger.info("front connected  ." + userSession.frontChannel);
		userSession.frontChannel.register(userSession.nioSelector, SelectionKey.OP_READ, userSession);
		// todo ,from config
		// 尝试连接Server 端口
		String serverIP = "localhost";
		int serverPort = 3306;
		InetSocketAddress serverAddress = new InetSocketAddress(serverIP, serverPort);
		userSession.backendChannel = SocketChannel.open();
		userSession.backendChannel.configureBlocking(false);
		userSession.backendChannel.connect(serverAddress);
		SelectionKey selectKey = userSession.backendChannel.register(userSession.nioSelector, SelectionKey.OP_CONNECT,
				userSession);
		userSession.backendKey = selectKey;
		logger.info("Connecting to server " + serverIP + ":" + serverPort);

	}

	public void onBackendConnect(UserSession userSession, boolean success, String msg) throws IOException {
		String logInfo = success ? " backend connect success " : "backend connect failed " + msg;
		logger.info(logInfo + " channel " + userSession.backendChannel);
		if (success) {
			InetSocketAddress serverRemoteAddr = (InetSocketAddress) userSession.backendChannel.getRemoteAddress();
			InetSocketAddress serverLocalAddr = (InetSocketAddress) userSession.backendChannel.getLocalAddress();
			userSession.backendAddr = "local port:" + serverLocalAddr.getPort() + ",remote "
					+ serverRemoteAddr.getHostString() + ":" + serverRemoteAddr.getPort();
			userSession.backendChannel.register(userSession.nioSelector, SelectionKey.OP_READ, userSession);
			// 如果发现前端有数据写入到后端的Buffer，就尝试转写到后端
			if (userSession.backendBuffer.isReadyToRead()) {
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

	public void handIO(UserSession userSession, SelectionKey selectionKey) {
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

	public boolean onFrontWriteEvent(UserSession userSession) throws IOException {
		return this.socketWriteFromBuf(true, userSession, userSession.frontChannel, userSession.frontBuffer);

	}

	public boolean onBackendWriteEvent(UserSession userSession) throws IOException {
		return this.socketWriteFromBuf(false, userSession, userSession.backendChannel, userSession.backendBuffer);
	}

	protected boolean socketReadtoBuf(boolean isFrontChannel, UserSession userSession, SocketChannel channel,
			ProxyBuffer otherProxyBuf) throws IOException {
		ByteBuffer buffer = otherProxyBuf.getBuffer();
		int read = channel.read(buffer);
		if (read == -1) {
			this.closeSocket(userSession, channel, true, "read EOF.");
			return true;
		}

		else if (read > 0) {
			// 表明NIO线程可以读取此Buffer的内容并写入对端对端Socket中发送出去
			buffer.flip();
			otherProxyBuf.setState(BufferState.READY_TO_READ);
		}
		return false;
	}

	protected void afterSocketRead(boolean socketDataEnd, UserSession userSession,ProxyBuffer proxyBuffer, SocketChannel otherChannel) throws IOException {
		if (socketDataEnd) {
			if (proxyBuffer.isReadyToWrite()) {// 表明对端Buffer的数据已经传输完成，可以关闭自己的连接了
				this.closeSocket(userSession, otherChannel, true, "data trans finished");
			}
			return;
		}
		if (proxyBuffer.isReadyToRead()) {
			// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
			modifySelectKey(userSession);
		}
	}

	protected boolean socketWriteFromBuf(boolean frontChannel, UserSession userSession, SocketChannel channel,
			ProxyBuffer proxyBuffer) throws IOException {
		boolean writeFinished = false;
		ByteBuffer buffer = proxyBuffer.getBuffer();
		channel.write(buffer);
		if (buffer.remaining() == 0) {
			writeFinished=true;
		}

		return writeFinished;
	}

	protected void afterSocketWrite(UserSession userSession, ProxyBuffer proxyBuffer) throws IOException {

		// 表明NIO线程可以读取对端Socket中发来的数据并且写入此Buffer中，等待下一次传输
		proxyBuffer.getBuffer().clear();
		proxyBuffer.setState(BufferState.READY_TO_WRITE);
		if (proxyBuffer.isReadyToWrite()) {
			modifySelectKey(userSession);
		}
	}

	private void closeSocket(UserSession userSession, SocketChannel channel, boolean normal, String msg) {
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
			this.onFrontSocketClosed(userSession, normal);
			userSession.frontChannel = null;
		} else if (channel == userSession.frontChannel) {
			this.onBackendSocketClosed(userSession, normal);
			userSession.backendChannel = null;
		}

	}

	/**
	 * 会同时修改Session中的前后端连接的NIO感兴趣事件
	 * 
	 * @param userSession
	 * @throws ClosedChannelException
	 */
	public void modifySelectKey(UserSession userSession) throws ClosedChannelException {
		if (userSession.frontKey != null && userSession.frontKey.isValid()) {
			int clientOps = 0;
			if (userSession.backendBuffer.isReadyToWrite())
				clientOps |= SelectionKey.OP_READ;
			if (userSession.frontBuffer.isReadyToRead())
				clientOps |= SelectionKey.OP_WRITE;
			userSession.frontKey.interestOps(clientOps);
		}
		if (userSession.backendKey != null && userSession.backendKey.isValid()) {
			int serverOps = 0;
			if (userSession.frontBuffer.isReadyToWrite())
				serverOps |= SelectionKey.OP_READ;
			if (userSession.backendBuffer.isReadyToRead())
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
	public void onFrontSocketClosed(UserSession userSession, boolean normal) {
		lazyCloseSession(userSession);
	}

	/**
	 * 后端连接关闭后，延迟关闭会话
	 * 
	 * @param userSession
	 * @param normal
	 */
	public void onBackendSocketClosed(UserSession userSession, boolean normal) {
		lazyCloseSession(userSession);
	}

	public void lazyCloseSession(UserSession userSession) {
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

	@Override
	public void onFrontReaded(UserSession userSession) throws IOException {
		boolean socketDataEnd = socketReadtoBuf(true, userSession, userSession.frontChannel, userSession.backendBuffer);
		this.afterSocketRead(socketDataEnd, userSession, userSession.backendBuffer,userSession.backendChannel);

	}

	@Override
	public void onBackendReaded(UserSession userSession) throws IOException {
		boolean socketDataEnd = socketReadtoBuf(false, userSession, userSession.backendChannel,
				userSession.frontBuffer);
		this.afterSocketRead(socketDataEnd, userSession, userSession.frontBuffer,userSession.frontChannel);

	}

	@Override
	public void onFrontWriteFinished(UserSession session) throws IOException {
		afterSocketWrite(session, session.frontBuffer);

	}

	@Override
	public void onBackendWriteFinished(UserSession session) throws IOException {
		afterSocketWrite(session, session.backendBuffer);

	}
}
