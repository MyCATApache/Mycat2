package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;

/**
 * 用来处理新的连接请求并创建Session
 * 
 * @author wuzhihui
 *
 */
public interface SessionManager<T extends Session> {

	/**
	 * 针对新创建的连接，产生对应的Session对象，
	 * @param bufPool 用来获取Buffer的Pool
	 * @param nioSelector 注册到对应的Selector上
	 * @param channel Channel对象
	 * @param isAcceptedCon 是服务端收到的请求，还是客户端发起的请求
	 * @return
	 * @throws IOException
	 */
	public T createSession(BufferPool bufPool, Selector nioSelector, SocketChannel channel,boolean isAcceptedCon) throws IOException;
	
	public Collection<T> getAllSessions();

	public void removeSession(Session session);
	
	
}
