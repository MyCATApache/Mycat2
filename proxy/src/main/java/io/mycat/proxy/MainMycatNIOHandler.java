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

import io.mycat.proxy.command.CommandHandler;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum  MainMycatNIOHandler implements NIOHandler<MycatSession> {
  INSTANCE;
  private static final Logger logger = LoggerFactory.getLogger(MainMycatNIOHandler.class);

  @Override
  public void onSocketRead(MycatSession mycat) throws IOException {
    mycat.currentProxyBuffer().newBufferIfNeed();
    if (!mycat.readFromChannel()) {
      return;
    }
    if (!mycat.readProxyPayloadFully()) {
      return;
    }
    CommandHandler.INSTANCE.handle(mycat);
    return;
  }

  @Override
  public void onSocketWrite(MycatSession mycat) throws IOException {
    mycat.writeToChannel();
  }

  @Override
  public void onWriteFinished(MycatSession mycat) throws IOException {
    if(mycat.isResponseFinished()){
      mycat.responseFinishedClear();
      mycat.resetPacket();
      mycat.change2ReadOpts();
    }else {
      mycat.writeToChannel();
    }
  }

  @Override
  public void onSocketClosed(MycatSession session, boolean normal) {
    session.close(normal, "");
  }

}
