package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 负责Proxy的NIO事件处理，需要为多个用户会话服务
 * 
 * @author wuzhihui
 *
 */
public interface NIOProxyHandler<T extends UserSession> {
	void onFrontConnected(BufferPool bufPool, Selector nioSelector, SocketChannel frontChannel) throws IOException;

	void onBackendConnect(T userSession, boolean success, String msg) throws IOException;

	void handIO(T userSession, SelectionKey key) throws IOException;

	void onFrontReaded(T session) throws IOException;

	void onBackendReaded(T session) throws IOException;

	void onFrontWriteFinished(T session) throws IOException;

	void onBackendWriteFinished(T session) throws IOException;
	
    void onFrontSocketClosed(T userSession, boolean normal);
    
    void onBackendSocketClosed(T userSession, boolean normal);
    
    void closeSocket(T userSession, SocketChannel channel, boolean normal, String msg) ;

}
