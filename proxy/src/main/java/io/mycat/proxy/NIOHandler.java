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
package io.mycat.proxy;

import io.mycat.proxy.session.Session;
import java.io.IOException;
import java.nio.channels.SelectionKey;

public interface NIOHandler<T extends Session> {

  default void onConnect(SelectionKey curKey, T session, boolean success, Throwable throwable)
      throws IOException {
    throw new RuntimeException("not implemented ");
  }

  void onSocketRead(T session) throws IOException;

  default void onSocketWrite(T session) throws IOException {
    session.writeToChannel();
  }

  void onWriteFinished(T session) throws IOException;

  void onSocketClosed(T session, boolean normal, String reasion);


  default <T extends Session<T>> T getSessionCaller() {
    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
    Session curSession = thread.getReactorEnv().getCurSession();
    return (T) curSession;
  }

}
