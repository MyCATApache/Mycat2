package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 此NIO Handler应该是单例的，能为多个Session会话服务
 * 
 * @author wuzhihui
 *
 */
public interface NIOHandler<T extends Session> {

	void onConnect(SelectionKey curKey, T session, boolean success, String msg) throws IOException;

	void onSocketRead(T session) throws IOException;

	void onSocketWrite(T session) throws IOException;

	public void onWriteFinished(T s) throws IOException;

	void onSocketClosed(T session,boolean normal);
}
