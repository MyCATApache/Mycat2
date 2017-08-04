package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

/**
 * 代表用户的会话，存放用户会话数据，如前端连接，后端连接，状态等数据
 * 
 * @author wuzhihui
 *
 */
public class UserSession {
	protected static Logger logger = Logger.getLogger(UserSession.class);	
	public BufferPool bufPool;
	public Selector nioSelector;
	// 前端连接
	public SocketChannel frontChannel;
	public SelectionKey frontKey;
	public ProxyBuffer frontBuffer;
	//后端连接
	public SocketChannel backendChannel;
	public SelectionKey backendKey;
	public ProxyBuffer backendBuffer;
	public UserSession(BufferPool bufPool,Selector nioSelector,SocketChannel frontChannel)
	{
		this.bufPool=bufPool;
		this.nioSelector=nioSelector;
		this.frontChannel=frontChannel;
		frontBuffer=new ProxyBuffer(bufPool.allocByteBuffer());
		backendBuffer=new ProxyBuffer(bufPool.allocByteBuffer());
	}
	
	public String sessionInfo()
	{
		return frontChannel+"->"+backendChannel;
	}

	public void close(String message) {
		 logger.info("close session "+this.sessionInfo()+ " for reason "+message);
			closeSocket(frontChannel);
			bufPool.recycleBuf(frontBuffer.getBuffer());
			closeSocket(backendChannel);
			bufPool.recycleBuf(this.backendBuffer.getBuffer());
		
	}
	private void closeSocket(Channel channel)
	{
		if(channel!=null)
		{
			try {
				channel.close();
			} catch (IOException e) {
				//
			}
			
		}
	}
}
