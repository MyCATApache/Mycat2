package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 用来处理新的连接请求并创建Session
 * 
 * @author wuzhihui
 *
 */
public interface SessionManager<T extends UserSession> {

	public T createSession(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException;
	
	
}
