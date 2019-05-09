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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Session<T extends Session> {

  final static Logger logger = LoggerFactory.getLogger(Session.class);

  public SocketChannel channel();

  boolean isClosed();

  public NIOHandler getCurNIOHandler();

  /**
   * 会话关闭时候的的动作，需要清理释放资源
   */
  void close(boolean normal, String hint);

  int sessionId();

  public void updateLastActiveTime();

  public void writeToChannel() throws IOException;

  public default void writeFinished(T session) throws IOException {
    session.getCurNIOHandler().onWriteFinished(session);
  }


  public boolean readFromChannel() throws IOException;

  public default void setCallBack(AsynTaskCallBack<T> callBack) {
    throw new MycatExpection("unsupport!");
  }

  public void setLastThrowable(Throwable e);

  public boolean hasError();

  public Throwable getLastThrowableAndReset();

  public void change2ReadOpts();

  public void clearReadWriteOpts();

  public void change2WriteOpts();


  public String getLastThrowableInfoTextAndReset();

  public default MycatReactorThread getMycatReactorThread() {
    Thread thread = Thread.currentThread();
    return (MycatReactorThread) thread;
  }

}
