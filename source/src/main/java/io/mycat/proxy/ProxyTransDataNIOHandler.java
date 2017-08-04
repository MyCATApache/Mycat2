
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
public class ProxyTransDataNIOHandler implements NIOProxyHandler {

	protected static Logger logger = Logger.getLogger(ProxyTransDataNIOHandler.class);

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
			userSession.backendChannel.register(userSession.nioSelector, SelectionKey.OP_READ, userSession);
			//如果发现前端有数据写入到后端的Buffer，就尝试转写到后端
			if (userSession.backendBuffer.isReadyToRead()) {
				boolean bufferWriteFinished = this.socketWriteFromBuf(false, userSession, userSession.backendChannel,
						userSession.backendBuffer);
				this.afterSocketWrite(bufferWriteFinished, userSession, userSession.backendChannel, userSession.backendBuffer, userSession.frontChannel);
			}
		} else {
			userSession.close("backend can't open:" + msg);
		}

	}

	public void handIO(UserSession userSession, SelectionKey selectionKey) {
		SocketChannel curChannel = (SocketChannel) selectionKey.channel();
		ProxyBuffer curChannelBuffer;
		ProxyBuffer otherChannelBuffer;
		SocketChannel otherChannel;
		boolean isFront = false;
		if (curChannel == userSession.frontChannel) {
			isFront = true;
			curChannelBuffer = userSession.frontBuffer;
			otherChannelBuffer = userSession.backendBuffer;
			otherChannel = userSession.backendChannel;
		} else {
			curChannelBuffer = userSession.backendBuffer;
			otherChannelBuffer = userSession.frontBuffer;
			otherChannel = userSession.frontChannel;
		}
		try {
			if (selectionKey.isReadable()) {
				boolean socketDataEnd = socketReadtoBuf(isFront, userSession, curChannel, otherChannelBuffer);
				this.afterSocketRead(socketDataEnd, userSession, curChannel, otherChannelBuffer, otherChannel);
			}
			if (selectionKey.isValid()&&selectionKey.isWritable()) {
				boolean bufferWriteFinished = this.socketWriteFromBuf(isFront, userSession, curChannel,
						curChannelBuffer);
				this.afterSocketWrite(bufferWriteFinished, userSession, curChannel, curChannelBuffer, otherChannel);
			}

		} catch (final Exception exception) {
			onSocketException(userSession, exception);
		}

	}

	protected boolean socketReadtoBuf(boolean frontChannel, UserSession userSession, SocketChannel channel,
			ProxyBuffer otherProxyBuf) throws IOException {
		ByteBuffer buffer = otherProxyBuf.getBuffer();
		int read = channel.read(buffer);
		if (read == -1) {
			String msg = frontChannel ? " front channel " : " backend channel ";
			logger.info(msg + "closed. " + userSession.sessionInfo());
			ProxyReactorThread.closeQuietly(channel);
			return true;
		}

		else if (read > 0) {
			// 表明NIO线程可以读取此Buffer的内容并写入对端对端Socket中发送出去
			buffer.flip();
			otherProxyBuf.setState(BufferState.READY_TO_READ);
		}
		return false;
	}

	protected void afterSocketRead(boolean socketDataEnd, UserSession userSession, SocketChannel readChannel,
			ProxyBuffer proxyBuffer, SocketChannel otherChannel) throws IOException {
		if (!otherChannel.isOpen()) {
			ProxyReactorThread.closeQuietly(readChannel);
			logger.info(" close this socket ,for peer data transfer socket is closed. " + userSession.sessionInfo());
			return;
		} else {
			if (socketDataEnd) {
				if (proxyBuffer.isReadyToWrite()) {// 表明对端Buffer的数据已经传输完成，可以关闭自己的连接了
					logger.info("close data peer socket ,for data trans finished ." + userSession.sessionInfo());
					ProxyReactorThread.closeQuietly(otherChannel);
				}
				return;
			}
			if (proxyBuffer.isReadyToRead()) {
				// 如果读到数据,修改NIO事件，自己不再读数据，对方则感兴趣写数据。
				modifySelectKey(userSession);
			}
		}
	}

	protected boolean socketWriteFromBuf(boolean frontChannel, UserSession userSession, SocketChannel channel,
			ProxyBuffer proxyBuffer) throws IOException {
		boolean writeFinished = false;
		ByteBuffer buffer = proxyBuffer.getBuffer();
		channel.write(buffer);
		if (buffer.remaining() == 0) {
			writeFinished = true;
			// 表明NIO线程可以读取对端Socket中发来的数据并且写入此Buffer中，等待下一次传输
			buffer.clear();
			proxyBuffer.setState(BufferState.READY_TO_WRITE);
		}

		return writeFinished;
	}

	protected void afterSocketWrite(boolean writeFinished, UserSession userSession, SocketChannel writingChannel,
			ProxyBuffer proxyBuffer, SocketChannel otherChannel) throws IOException {
		if (!otherChannel.isOpen()) {
			ProxyReactorThread.closeQuietly(writingChannel);
			logger.info(" close this socket ,for peer data transfer socket is closed. " + userSession.sessionInfo());
			return;
		} else {
			if (proxyBuffer.isReadyToWrite()) {
				modifySelectKey(userSession);
			}
		}
	}

	/**
	 * 会同时修改Session中的前后端连接的NIO感兴趣事件
	 * 
	 * @param userSession
	 * @throws ClosedChannelException
	 */
	private void modifySelectKey(UserSession userSession) throws ClosedChannelException {
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
}
