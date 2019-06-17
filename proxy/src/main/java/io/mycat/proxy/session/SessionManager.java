/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session;

import io.mycat.buffer.BufferPool;
import io.mycat.proxy.callback.SessionCallBack;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;

/**
 * @author jamie12221 chen junwen date 2019-05-10 21:13 Session管理器
 **/
public interface SessionManager<T extends Session> {


  /**
   * 返回所有的Session，此方法可能会消耗性能，如果仅仅统计数量，不建议调用此方法
   */
  Collection<T> getAllSessions();

  /**
   * 获取当前Session数量
   *
   * @return count
   */
  int currentSessionCount();

  /**
   * 从管理器中移除Session
   */
  void removeSession(T session, boolean normal, String reason);

  /**
   * 前端session管理器,前端session管理器,收到的通道就是已经连接的
   */
  interface FrontSessionManager<T extends Session> extends SessionManager<T> {

    void acceptNewSocketChannel(Object keyAttachement, BufferPool bufPool, Selector selector,
        SocketChannel socketChannel) throws IOException;
  }

  /**
   * 后端session管理器
   *
   * @param <T> session实现
   * @param <KEY> 根据此key查询session
   */
  interface BackendSessionManager<T extends Session, KEY> extends SessionManager<T> {

    void getIdleSessionsOfIds(KEY key,int[] ids, SessionCallBack<T> asyncTaskCallBack);

    /**
     * 根据key获取闲置连接,如果没有闲置连接则创建新的连接
     */
    void getIdleSessionsOfKey(KEY key, SessionCallBack<T> asyncTaskCallBack);

    /**
     * 把session放入闲置池
     */
    void addIdleSession(T Session);

    /**
     * 创建新的连接,创建新的连接后,该连接必须是立即使用的,所以不会加入到闲置池
     */
    void createSession(KEY key, SessionCallBack<T> callBack);

    /**
     * 根据此key关闭连接
     */
    void clearAndDestroyDataSource(KEY key, String reason);

    /**
     * 空闲连接检查与关闭
     */
    void idleConnectCheck();
  }

}
