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

import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.handler.NIOHandler;
import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * @author jamie12221 chen junwen
 * @date 2019-05-10 21:13 Session
 **/
public interface Session<T extends Session> {

  /**
   * 通道
   */
  SocketChannel channel();

  /**
   * 判断session是否已经关闭,一般采用通道关闭来判断,判断该方法可以实现关闭幂等
   */
  boolean isClosed();

  /**
   * 获取1当前session的处理句柄
   */
  NIOHandler getCurNIOHandler();

  /**
   * 会话关闭时候的的动作，需要清理释放资源
   */
  default void close(boolean normal, String hint) {
    getCurNIOHandler().onSocketClosed(this, normal, hint);
  }

  /**
   * 该session的标识符,唯一
   */
  int sessionId();

  /**
   * 获取该session,最近活跃的时间
   */
  void updateLastActiveTime();

  /**
   * 把session内buffer写入通道
   */
  void writeToChannel() throws IOException;

  /**
   * session内buffer写入通道,根据一定条件判断数据写入完毕后回调方法
   */
  default void writeFinished(T session) throws IOException {
    session.getCurNIOHandler().onWriteFinished(session);
  }

  /**
   * 读事件回调
   */
  boolean readFromChannel() throws IOException;

  /**
   * 设置回调函数,若果设置了回调,则该session的资源释放取决于回调代码什么时候结束,
   */
  void setCallBack(AsyncTaskCallBack<T> callBack);

  /**
   * 注册读事件
   */
  void change2ReadOpts();

  /**
   * 清除读写事件
   */
  void clearReadWriteOpts();

  /**
   * 注册写事件
   */
  void change2WriteOpts();

  /**
   * 获取当前线程池
   */
  default MycatReactorThread getMycatReactorThread() {
    Thread thread = Thread.currentThread();
    return (MycatReactorThread) thread;
  }

  /**
   * 获取上下文设置的错误信息
   */
  String getLastMessage();

  String setLastMessage(String lastMessage);

  static String getString(Throwable e) {
    StringBuilder sb = new StringBuilder();
    sb.append(e.toString());
    sb.append("\n");
    StackTraceElement[] stackArray = e.getStackTrace();
    for (int i = 0; i < stackArray.length; i++) {
      StackTraceElement element = stackArray[i];
      sb.append(element.toString()).append("\n");
    }
    return sb.toString();
  }

  default String setLastMessage(Throwable e) {
    String string = getString(e);
    setLastMessage(string);
    return string;
  }

  default long currentTimeMillis() {
    return getMycatReactorThread().getLastActiveTime();
  }
}
