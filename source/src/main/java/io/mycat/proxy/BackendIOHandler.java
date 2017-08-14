package io.mycat.proxy;

import java.io.IOException;

/**
 * 后端IO的Handler，如果是客户端主动向对方Server发起连接，通常应该实现这个接口去处理NIO
 * 
 * @author wuzhihui
 *
 */
public interface BackendIOHandler<T extends Session>  extends NIOHandler<T> {

	void onBackendConnect(T userSession, boolean success, String msg) throws IOException;

	void onBackendRead(T session) throws IOException;

	void onBackendWrite(T session) throws IOException;

	void onBackendSocketClosed(T userSession, boolean normal);

}
