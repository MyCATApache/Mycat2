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
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.task.AsynTaskCallBack;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;

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
	int curSessionCount();

	/**
	 * 获取默认的Session处理句柄
	 * @return
	 */
	NIOHandler<T> getDefaultSessionHandler();

	/**
	 * 从管理器中移除Session
	 * @param session
	 */
	void removeSession(T session);

	interface FrontSessionManager<T extends Session> extends SessionManager<T> {

		T acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool, Selector selector,
				SocketChannel socketChannel) throws IOException;
	}

	interface BackendSessionManager<T extends Session, ARG> extends SessionManager<T> {

		void getIdleSessionsOfKey(ARG key, AsynTaskCallBack<T> asynTaskCallBack);

		void addIdleSession(T Session);

		void removeIdleSession(T Session);

		void createSession(ARG key, AsynTaskCallBack<T> callBack);

		void clearAndDestroyMySQLSession(ARG dsMetaBean, String reason);
	}

}
