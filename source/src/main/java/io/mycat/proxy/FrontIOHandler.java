package io.mycat.proxy;

import java.io.IOException;
/**
 * 处理前端连接请求的NIOHandler
 * @author wuzhihui
 *
 * @param <T>
 */
public interface FrontIOHandler<T extends Session> extends NIOHandler<T> {
	
	
	/**
	 * 进行前端通道数据的读取
	 * @param session 会话对象
	 * @throws IOException
	 */
	void onFrontRead(T session) throws IOException;

	/**
	 * 前端通道数据的写入
	 * @param session 会话对象
	 * @throws IOException
	 */
	void onFrontWrite(T session) throws IOException;

	/**
	 * 进行前端通道的关闭处理
	 * @param userSession 会话对象
	 * @param normal 暂时无用
	 */
	void onFrontSocketClosed(T userSession, boolean normal);
}
