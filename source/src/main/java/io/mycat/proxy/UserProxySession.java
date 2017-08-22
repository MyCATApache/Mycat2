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
		if (proxyBuf.writeIndex > buffer.capacity() * 1 / 3) {
			proxyBuf.compact();
		}else{
			// buffer.position 在有半包没有参与透传时,会小于 writeIndex。
			// 大部分情况下   position == writeIndex
			buffer.position(proxyBuf.writeIndex);
		}
		
		int readed = channel.read(buffer);
		logger.debug(" readed {} total bytes ", readed);
		if (readed == -1) {
			logger.warn("Read EOF ,socket closed ");
			throw new ClosedChannelException();
		} else if (readed == 0) {
			logger.warn("readed zero bytes ,Maybe a bug ,please fix it !!!!");
		}
		proxyBuf.writeIndex = buffer.position();
		return readed > 0;
	}

	/**
	 * 从内部Buffer数据写入到SocketChannel中发送出去，readState里记录了写到Socket中的数据指针位置 方法，
	 * 
	 * @param channel
	 */
	public void writeToChannel(ProxyBuffer proxyBuf, SocketChannel channel) throws IOException {
		
		ByteBuffer buffer = proxyBuf.getBuffer();
		buffer.limit(proxyBuf.readIndex);
		buffer.position(proxyBuf.readMark);
		int writed = channel.write(buffer);
		proxyBuf.readMark += writed;   //记录本次磁轭如到 Channel 中的数据
		if(!buffer.hasRemaining()){
			logger.debug("writeToChannel write  {} bytes ",writed);
			// buffer 中需要透传的数据全部写入到 channel中后,会进入到当前分支.这时 readIndex == readLimit
			if(proxyBuf.readMark != proxyBuf.readIndex){
				logger.error("writeToChannel has finished but readIndex != readLimit, please fix it !!!");
			}
			if (proxyBuf.readIndex > buffer.capacity() * 2 / 3) {
				proxyBuf.compact();
			}else{
				buffer.limit(buffer.capacity());
			}
			//切换读写状态
			proxyBuf.flip();
			/*
			 * 如果需要自动切换owner,进行切换  
			 * 1. writed==0 或者  buffer 中数据没有写完时,注册可写事件 时,会进行owner 切换 注册写事件,完成后,需要自动切换回来
			 */
			if(proxyBuf.needAutoChangeOwner()){
				proxyBuf.changeOwner(!proxyBuf.frontUsing()).setPreUsing(null);
			}
		}else{
			/**
			 * 1. writed==0 或者  buffer 中数据没有写完时,注册可写事件
			 *    通常发生在网络阻塞或者 客户端  COM_STMT_FETCH 命令可能会 出现没有写完或者 writed == 0 的情况
			 */
			logger.debug("register OP_WRITE  selectkey .write  {} bytes. current channel is {}",writed,channel);
			//需要切换 owner ,同时保存当前 owner 用于数据传输完成后,再切换回来
			// proxyBuf 读写状态不切换,会切换到相同的事件,不会重复注册
			proxyBuf.setPreUsing(proxyBuf.frontUsing())
					.changeOwner(!proxyBuf.frontUsing());
		}
		modifySelectKey();
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
		SelectionKey theKey = this.frontBuffer.frontUsing() ? frontKey : backendKey;
		int clientOps = SelectionKey.OP_READ;
		if (theKey != null && theKey.isValid()) {
			if (frontBuffer.isInWriting() == false) {
				clientOps = SelectionKey.OP_WRITE;
			}
			int oldOps = theKey.interestOps();
			if (oldOps != clientOps) {
				theKey.interestOps(clientOps);
			}
		}
		logger.info(" current selectkey is {},channel is {}",theKey.interestOps(),theKey.channel());
		//取消对端 读写事件
		SelectionKey otherKey = this.frontBuffer.frontUsing() ? backendKey : frontKey;
		if(otherKey!=null&&otherKey.isValid()){
			otherKey.interestOps(otherKey.interestOps() & ~(SelectionKey.OP_WRITE | SelectionKey.OP_READ));
			logger.info(" other selectkey is {},channel is {}",otherKey.interestOps(),otherKey.channel());
		}
		
	}
}
