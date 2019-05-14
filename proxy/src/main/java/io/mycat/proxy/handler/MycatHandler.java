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

import io.mycat.proxy.session.MycatSession;
import java.io.IOException;

public enum MycatHandler implements NIOHandler<MycatSession> {
  INSTANCE;

  final
  @Override
  public void onSocketRead(MycatSession mycat) throws IOException {
    mycat.currentProxyBuffer().newBufferIfNeed();
    if (!mycat.readFromChannel()) {
      return;
    }
    if (!mycat.readProxyPayloadFully()) {
      return;
    }
    mycat.handle();
    return;
  }

  @Override
  public void onWriteFinished(MycatSession mycat) throws IOException {
    if (mycat.isResponseFinished()) {
      mycat.onHandlerFinishedClear(true);
      mycat.change2ReadOpts();
    } else {
      mycat.writeToChannel();
    }
  }

  /**
   * 1.mycat session 不存在切换 handler的情况 2.onSocketClosed是session的close方法,它完成了整个状态清理与关闭,所以onSocketClosed无需实现
   */
  @Override
  public void onSocketClosed(MycatSession session, boolean normal, String reason) {

  }

  /**
   * mycat session写入处理
   */
  public interface MycatSessionWriteHandler {

    void writeToChannel(MycatSession session) throws IOException;

    void onWriteFinished(MycatSession session) throws IOException;
  }

}
