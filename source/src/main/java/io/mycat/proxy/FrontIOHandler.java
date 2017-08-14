package io.mycat.proxy;

import java.io.IOException;
/**
 * 处理前端连接请求的NIOHandler
 * @author wuzhihui
 *
 * @param <T>
 */
public interface FrontIOHandler<T extends Session> extends NIOHandler<T> {
	void onFrontRead(T session) throws IOException;

	void onFrontWrite(T session) throws IOException;

	void onFrontSocketClosed(T userSession, boolean normal);
}
