package io.mycat.proxy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
	public ProxyBuffer frontBuffer;

	// 后端连接
	public String backendAddr;
	public SocketChannel backendChannel;
	public SelectionKey backendKey;

	public UserProxySession(BufferPool bufferPool, Selector selector, SocketChannel channel) throws IOException {
		super(bufferPool, selector, channel);
		frontBuffer = new ProxyBuffer(bufPool.allocByteBuffer());
	}

	/**
	 * 从SocketChannel中读取数据并写入到内部Buffer中,writeState里记录了写入的位置指针
	 * 第一次调用之前需要确保Buffer状态为Write状态，并指定要写入的位置，
	 * 
	 * @param channel
	 * @return 读取了多少数据
	 */
	public boolean readFromChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {

		ByteBuffer buffer = proxyBuf.getBuffer();
		buffer.limit(proxyBuf.writeState.optLimit);
		buffer.position(proxyBuf.writeState.optPostion);
		int readed = channel.read(buffer);
		logger.debug(" readed {} total bytes ,channel {}", readed, channel);
		proxyBuf.writeState.curOptedLength = readed;
		if (readed > 0) {
			proxyBuf.writeState.optPostion += readed;
			proxyBuf.writeState.optedTotalLength += readed;
			proxyBuf.readState.optLimit = proxyBuf.writeState.optPostion;
		} else if (readed == -1) {
			logger.warn("Read EOF ,socket closed ");
			throw new ClosedChannelException();
		} else if (readed == 0) {
			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		return readed > 0;
	}

	/**
	 * 从内部Buffer数据写入到SocketChannel中发送出去，readState里记录了写到Socket中的数据指针位置 方法，
	 * 
	 * @param channel
	 */
	public void writeToChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {
		ByteBuffer buffer = proxyBuf.getBuffer();
		BufferOptState readState = proxyBuf.readState;
		BufferOptState writeState = proxyBuf.writeState;
		buffer.position(readState.optPostion);
		buffer.limit(readState.optLimit);
		int writed = channel.write(buffer);
		readState.curOptedLength = writed;
		readState.optPostion += writed;
		readState.optedTotalLength += writed;
		if (buffer.remaining() == 0) {
			if (writeState.optPostion > buffer.position()) {
				// 当前Buffer中写入的数据多于透传出去的数据，因此透传并未完成
				// compact buffer to head
				buffer.limit(writeState.optPostion);
				buffer.compact();
				readState.optPostion = 0;
				readState.optLimit = buffer.position();
				writeState.optPostion = buffer.position();
				// 继续从对端Socket读数据

			} else {
				// 数据彻底写完，切换为读模式，对端读取数据
				proxyBuf.changeOwner(!proxyBuf.frontUsing());
				proxyBuf.flip();
				modifySelectKey();
			}
		}
	}

	/**
	 * 手动创建的ProxyBuffer需要手动释放，recycleAllocedBuffer()
	 * 
	 * @return ProxyBuffer
	 */
	public ProxyBuffer allocNewProxyBuffer() {
		logger.info("alloc new ProxyBuffer ");
		return new ProxyBuffer(bufPool.allocByteBuffer());
	}

	/**
	 * 释放手动分配的ProxyBuffer
	 * 
	 * @param curFrontBuffer
	 */
	public void recycleAllocedBuffer(ProxyBuffer curFrontBuffer) {
		logger.info("recycle alloced ProxyBuffer ");

		if (curFrontBuffer != null) {
			this.bufPool.recycleBuf(curFrontBuffer.getBuffer());
		}
	}

	public boolean isBackendOpen() {
		return backendChannel != null && backendChannel.isConnected();
	}

	public String sessionInfo() {
		return " [" + this.frontAddr + "->" + this.backendAddr + ']';
	}

	@SuppressWarnings("rawtypes")
	public void lazyCloseSession(final boolean normal,final String reason) {
		if (isClosed()) {
			return;
		}

		ProxyRuntime.INSTANCE.addDelayedNIOJob(() -> {
			if (!isClosed()) {
				close(normal,reason);
			}
		}, 10, (ProxyReactorThread) Thread.currentThread());
	}

	public void close(boolean normal,String hint){
		if (!this.isClosed()) {
			bufPool.recycleBuf(frontBuffer.getBuffer());
			// 关闭后端连接
			closeSocket(backendChannel,normal,hint);
			super.close(normal,hint);
		} else {
			super.close(normal,hint);
		}

	}

	public boolean hasDataTrans2Backend() {
		return frontBuffer.backendUsing() && frontBuffer.isInReading();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void closeSocket(SocketChannel channel, boolean normal, String msg) {
		super.closeSocket(channel, normal, msg);
		if (channel == frontChannel) {
			((FrontIOHandler) getCurNIOHandler()).onFrontSocketClosed(this, normal);
			frontChannel = null;
		} else if (channel == backendChannel) {
			((BackendIOHandler) getCurNIOHandler()).onBackendSocketClosed(this, normal);
			backendChannel = null;
		}

	}

	public void modifySelectKey() throws ClosedChannelException {
	    //int op = backendKey.interestOps();
	  //  boolean flag = backendKey.isConnectable();
		if (frontKey != null && frontKey.isValid()) {
			int clientOps = SelectionKey.OP_READ;
			if (frontBuffer.isInWriting() == false) {
				clientOps = SelectionKey.OP_WRITE;
			}
			frontKey.interestOps(clientOps);
			backendChannel = backendChannel;
		}
	}
}
