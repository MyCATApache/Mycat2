package io.mycat.proxy;

import java.io.IOException;
import java.nio.channels.SelectionKey;

/**
 * 处理 程序中主动发起连接(Connect)的IO连接事件
 * 
 * @author wuzhihui
 *
 * @param <T>
 */
public interface ConnectIOHandler<T extends Session> {
	/**
	 * 处理Connect事件
	 * 
	 * @param userSession
	 * @param success
	 *            是否连接成功
	 * @param msg
	 * @throws IOException
	 */
	void onConnect(SelectionKey curKey, T userSession, boolean success, String msg) throws IOException;
}
