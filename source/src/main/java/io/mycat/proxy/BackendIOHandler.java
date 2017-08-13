package io.mycat.proxy;

import java.io.IOException;

/**
 * 后端IO的Handler
 * 
 * @author wuzhihui
 *
 */
public interface BackendIOHandler<T extends UserProxySession>  extends NIOHandler<T> {

	/**
	 * 处理连接事件
	 * @param userSession 会话对象
	 * @param success  成功失败标识中
	 * @param msg 如果为错误，则为错误提示信息
	 * @throws IOException
	 */
	void onBackendConnect(T userSession, boolean success, String msg) throws IOException;

	/**
	 * 后端的事件读取
	 * @param session
	 * @throws IOException
	 */
	void onBackendRead(T session) throws IOException;

	/**
	 * 后端事件数据的写入
	 * @param session
	 * @throws IOException
	 */
	void onBackendWrite(T session) throws IOException;

	/**
	 * 进行socket关闭
	 * @param userSession
	 * @param normal
	 */
	void onBackendSocketClosed(T userSession, boolean normal);

}
