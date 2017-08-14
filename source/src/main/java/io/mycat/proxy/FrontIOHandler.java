package io.mycat.proxy;

import java.io.IOException;

/**
 * 前端通道IO事件接口
 * @since 2017年8月13日 下午3:31:31
 * @version 0.0.1
 * @author liujun
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
