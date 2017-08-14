package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 代表用户的会话，存放用户会话数据，如前端连接，后端连接，状态等数据
 * 
 * @author wuzhihui
 *
 */
public class UserProxySession extends AbstractSession {
	public UserProxySession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel);
	}

	// 后端连接
	public String backendAddr;
	public SocketChannel backendChannel;
	public SelectionKey backendKey;

	public boolean isBackendOpen() {
		return backendChannel != null && backendChannel.isConnected();
	}

	public String sessionInfo() {
		return " [" + this.frontAddr + "->" + this.backendAddr + ']';
	}

	@SuppressWarnings("rawtypes")
	public void lazyCloseSession(final String reason) {
		if (isClosed()) {
			return;
		}

		ProxyRuntime.INSTANCE.addDelayedNIOJob(() -> {
			if (!isClosed()) {
				close(reason);
			}
		}, 10, (ProxyReactorThread) Thread.currentThread());
	}

	public void close(String message) {
		if (!this.isClosed()) {
			super.close(message);
			// 关闭后端连接
			closeSocket(backendChannel);
			super.close(message);
		} else {
			super.close(message);
		}

	}

	public boolean hasDataTrans2Backend() {
		return frontBuffer.backendUsing() && frontBuffer.isInReading();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void closeSocket(SocketChannel channel, boolean normal, String msg) {
		if (channel == null) {
			return;
		}
		String logInf = ((normal) ? " normal close " : "abnormal close ")
				+ ((channel == frontChannel) ? " front socket. " : " backend socket. ");
		logger.info(logInf + sessionInfo() + "  reason:" + msg);
		try {
			channel.close();
		} catch (IOException e) {
		}
		if (channel == frontChannel) {
			((FrontIOHandler) getCurNIOHandler()).onFrontSocketClosed(this, normal);
			frontChannel = null;
		} else if (channel == backendChannel) {
			((BackendIOHandler) getCurNIOHandler()).onBackendSocketClosed(this, normal);
			backendChannel = null;
		}

	}

}
