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

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.task.AsyncTaskCallBack;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;

/**
 * @author jamie12221 chen junwen
 * @date 2019-05-10 21:13 Session管理器
 **/
public interface SessionManager<T extends Session> {


	/**
	 * 返回所有的Session，此方法可能会消耗性能，如果仅仅统计数量，不建议调用此方法
	 * 
	 * @return
	 */
	Collection<T> getAllSessions();

	/**
	 * 获取当前Session数量
	 * @return count
	 */
  int currentSessionCount();

	/**
	 * 从管理器中移除Session
	 * @param session
	 */
  void removeSession(T session, boolean normal, String reason);

	/**
	 * 前端session管理器,前端session管理器,收到的通道就是已经连接的
	 */
	interface FrontSessionManager<T extends Session> extends SessionManager<T> {

		T acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool, Selector selector,
				SocketChannel socketChannel) throws IOException;
	}

	/**
	 * 后端session管理器
	 * @param <T> session实现
	 * @param <KEY> 根据此key查询session
	 */
	interface BackendSessionManager<T extends Session, KEY> extends SessionManager<T> {

		/**
		 * 根据key获取闲置连接,如果没有闲置连接则创建新的连接
		 */
		void getIdleSessionsOfKey(KEY key, AsyncTaskCallBack<T> asyncTaskCallBack);

		/**
		 * 把session放入闲置池
		 */
		void addIdleSession(T Session);

		/**
		 * 创建新的连接,创建新的连接后,该连接必须是立即使用的,所以不会加入到闲置池
		 * @param key
		 * @param callBack
		 */
		void createSession(KEY key, AsyncTaskCallBack<T> callBack);

		/**
		 * 根据此key关闭连接
		 */
		void clearAndDestroyDataSource(KEY key, String reason);
	}

}
