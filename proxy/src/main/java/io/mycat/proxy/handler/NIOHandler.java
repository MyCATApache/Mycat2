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
package io.mycat.proxy.handler;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.Session;

/**
 * @author chen junwen
 */
public interface NIOHandler<T extends Session> {



  /**
   * 回调读事件
   * @param session
   */
  void onSocketRead(T session);

  /**
   * 通道可写回调,在此方法内实际上是把session的数据写入通道,然后判断写入数据后,写入是否结束
   * @param session
   */
  void onSocketWrite(T session);

  /**
   * 在session里面回调的写入完成事件,而非selector回调事件
   * @param session
   */
  void onWriteFinished(T session);

  // void onClear(T session);

  void onException(T session, Exception e);

  default ProxyRuntime getRuntime(){
    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
    return thread.getRuntime();
  }
}
