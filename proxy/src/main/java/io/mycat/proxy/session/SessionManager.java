/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
