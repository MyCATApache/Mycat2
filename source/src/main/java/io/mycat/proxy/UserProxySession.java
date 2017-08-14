package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 代表用户的会话，存放用户会话数据，如前端连接，后端连接，状态等数据
 * Proxy模式模式下，通常一端的Socket会把读到的数据写入到对端的Buffer里，即前端收到的数据放入到BackendBuffer，随后被Backchannel写入并发送出去；
 * 类似的，后端收到的数据会放入到frontBuffer里，随后被frontChannel写入并发送出去。
 * 
 * @author wuzhihui
 *
 */
public class UserProxySession extends AbstractSession {
	// 保存前端收到的数据，即相当于前端的ReadingBuffer，channel.read(backendBuffer)
	public ProxyBuffer backendBuffer;

	public UserProxySession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel);
		backendBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
	}

	public enum NetOptMode {
		// 直接透传此报文到对端，只处理前端读写，只处理后端读写
		DirectTrans, FrontRW, BackendRW
	}

	// 后端连接
	public String backendAddr;
	public SocketChannel backendChannel;
	public SelectionKey backendKey;

	/**
	 * 默认是透传报文模式，联动两边的NIO事件
	 */
	public NetOptMode netOptMode = NetOptMode.DirectTrans;

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
			bufPool.recycleBuf(backendBuffer.getBuffer());
			super.close(message);
		} else {
			super.close(message);
		}

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

	/**
	 * 根据网络模式确定是否修改前端连接与后端连接的NIO感兴趣事件
	 * 
	 * @param userSession
	 * @throws ClosedChannelException
	 */

	public void modifySelectKey() throws ClosedChannelException {
		boolean frontKeyNeedUpdate = false;
		boolean backKeyNeedUpdate = false;
		switch (netOptMode) {
		//检查是否为透传模式
		case DirectTrans: {
			frontKeyNeedUpdate = true;
			backKeyNeedUpdate = true;
			break;
		}
		//只处理前端读写
		case FrontRW: {
			frontKeyNeedUpdate = true;
			backKeyNeedUpdate = false;
			break;
		}
		//只处理后端读写
		case BackendRW: {
			frontKeyNeedUpdate = false;
			backKeyNeedUpdate = true;
			break;
		}
		}
		if (frontKeyNeedUpdate && frontKey != null && frontKey.isValid()) {
			int clientOps = 0;
			if (backendBuffer.isInWriting())
				clientOps |= SelectionKey.OP_READ;
			if (frontBuffer.isInWriting() == false)
				clientOps |= SelectionKey.OP_WRITE;
			frontKey.interestOps(clientOps);
		}
		if (backKeyNeedUpdate && backendKey != null && backendKey.isValid()) {
			int serverOps = 0;
			if (frontBuffer.isInWriting())
				serverOps |= SelectionKey.OP_READ;
			if (backendBuffer.isInWriting() == false)
				serverOps |= SelectionKey.OP_WRITE;
			backendKey.interestOps(serverOps);
		}
	}

}
