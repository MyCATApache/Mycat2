package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 负责Proxy的NIO事件处理，需要为多个用户会话服务
 * 
 * @author wuzhihui
 *
 */
public interface NIOProxyHandler {
	void onFrontConnected(UserSession userSession) throws IOException;

	void onBackendConnect(UserSession userSession, boolean success, String msg) throws IOException;

	void handIO(UserSession userSession, SelectionKey key) throws IOException;

	void onFrontReaded(UserSession session) throws IOException;

	void onBackendReaded(UserSession session) throws IOException;

	void onFrontWriteFinished(UserSession session) throws IOException;

	void onBackendWriteFinished(UserSession session) throws IOException;
	
    void onFrontSocketClosed(UserSession userSession, boolean normal);
    
    void onBackendSocketClosed(UserSession userSession, boolean normal);

}
