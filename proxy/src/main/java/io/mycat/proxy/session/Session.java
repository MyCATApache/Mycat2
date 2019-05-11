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

import io.mycat.MycatExpection;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.task.AsynTaskCallBack;
import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface Session<T extends Session> {


  SocketChannel channel();

  boolean isClosed();

  NIOHandler getCurNIOHandler();

  /**
   * 会话关闭时候的的动作，需要清理释放资源
   */
  void close(boolean normal, String hint);

  int sessionId();

  void updateLastActiveTime();

  void writeToChannel() throws IOException;

  default void writeFinished(T session) throws IOException {
    session.getCurNIOHandler().onWriteFinished(session);
  }


  boolean readFromChannel() throws IOException;

  default void setCallBack(AsynTaskCallBack<T> callBack) {
    throw new MycatExpection("unsupport!");
  }

  void setLastThrowable(Throwable e);

  boolean hasError();

  Throwable getLastThrowableAndReset();

  void change2ReadOpts();

  void clearReadWriteOpts();

  void change2WriteOpts();


  String getLastThrowableInfoTextAndReset();

  default MycatReactorThread getMycatReactorThread() {
    Thread thread = Thread.currentThread();
    return (MycatReactorThread) thread;
  }


}
