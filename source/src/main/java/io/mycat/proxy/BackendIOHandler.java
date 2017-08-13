package io.mycat.proxy;

import java.io.IOException;

/**
 * 后端IO的Handler
 * 
 * @author wuzhihui
 *
 */
public interface BackendIOHandler<T extends UserProxySession>  extends NIOHandler<T> {

	void onBackendConnect(T userSession, boolean success, String msg) throws IOException;

	void onBackendRead(T session) throws IOException;

	void onBackendWrite(T session) throws IOException;

	void onBackendSocketClosed(T userSession, boolean normal);

}
