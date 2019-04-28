package io.mycat.proxy.session;

import io.mycat.proxy.NIOHandler;

import java.util.Collection;

public interface SessionManager<T extends Session> {


	/**
	 * 返回所有的Session，此方法可能会消耗性能，如果仅仅统计数量，不建议调用此方法
	 * 
	 * @return
	 */
	public Collection<T> getAllSessions();

	/**
	 * 获取当前Session数量
	 * @return count
	 */
	public int curSessionCount();

	/**
	 * 获取默认的Session处理句柄
	 * @return
	 */
	public NIOHandler<T> getDefaultSessionHandler();

	/**
	 * 从管理器中移除Session
	 * @param session
	 */
	public void removeSession(T session);

}
