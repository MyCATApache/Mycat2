package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * 负责Proxy的NIO事件处理，需要为多个用户会话服务
 * 
 * @author wuzhihui
 *
 */
public interface NIOProxyHandler<T extends UserSession> {

	void onBackendConnect(T userSession, boolean success, String msg) throws IOException;

	void onFrontRead(T session) throws IOException;

	void onFrontWrite(T session) throws IOException;

	void onBackendRead(T session) throws IOException;

	void onBackendWrite(T session) throws IOException;

	void onFrontSocketClosed(T userSession, boolean normal);

	void onBackendSocketClosed(T userSession, boolean normal);

}
