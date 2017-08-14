package io.mycat.proxy;

import java.nio.channels.SocketChannel;

/**
 * 代表一个会话
 * 
 * @author wuzhihui
 *
 */
public interface Session {
	/**
	 * 获取前端连接，前端连接为客户端主动连接到本Server的连接
	 * 
	 * @return
	 */
	public SocketChannel frontChannel();

	boolean isClosed();

	public <T extends Session> SessionManager<T> getMySessionManager();

	// 当前NIO ProxyHandler
	public <T extends Session> NIOHandler<?> getCurNIOHandler();
	
	/**
	 * 会话关闭时候的的动作，需要清理释放资源
	 * @param message
	 */
	public void close(String message);

}
