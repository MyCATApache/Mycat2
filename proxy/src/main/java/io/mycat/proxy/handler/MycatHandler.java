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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The enum Mycat handler.
 */
public enum MycatHandler implements NIOHandler<MycatSession> {
  /**
   * Instance mycat handler.
   */
  INSTANCE;
  private static final Logger LOGGER = LoggerFactory.getLogger(MycatHandler.class);

  final
  @Override
  public void onSocketRead(MycatSession mycat) {
    if (!mycat.isOpen()) {
      onException(mycat, new ClosedChannelException());
      mycat.close(false, "mysql session has closed");
      return;
    }
    try {
      mycat.currentProxyBuffer().newBufferIfNeed();
      if (!mycat.readFromChannel()) {
        return;
      }
      if (!mycat.readProxyPayloadFully()) {
        return;
      }
      mycat.handle();
      return;
    } catch (ClosedChannelException e) {
      onException(mycat, e);
      return;
    } catch (Exception e) {
      onClear(mycat);
      mycat.setLastMessage(e.toString());
      mycat.writeErrorEndPacketBySyncInProcessError();
      onException(mycat, e);
    }
  }

  @Override
  public void onSocketWrite(MycatSession mycat) {
    try {
      if ((mycat.getChannelKey().interestOps()& SelectionKey.OP_WRITE)!=0){
        mycat.writeToChannel();
      }
    } catch (Exception e) {
      onClear(mycat);
      mycat.close(false, e);
    }
  }

  @Override
  public void onWriteFinished(MycatSession mycat) {
    try {
      if (mycat.isResponseFinished()) {
        mycat.onHandlerFinishedClear();
        mycat.change2ReadOpts();
      } else {
        mycat.writeToChannel();
      }
    } catch (Exception e) {
      onClear(mycat);
      mycat.close(false, e);
    }
  }

  @Override
  public void onException(MycatSession mycat, Exception e) {
    LOGGER.error("{}", e);
    MycatSessionWriteHandler writeHandler = mycat.getWriteHandler();
    if (writeHandler != null) {
      writeHandler.onException(mycat, e);
    }
    onClear(mycat);
    mycat.close(false, e.toString());
  }

  /**
   * On clear.
   *
   * @param session the session
   */
  public void onClear(MycatSession session) {
    session.onHandlerFinishedClear();
  }


  /**
   * mycat session写入处理
   */
  public interface MycatSessionWriteHandler {

    /**
     * Write to channel.
     *
     * @param session the session
     * @throws IOException the io exception
     */
    void writeToChannel(MycatSession session) throws IOException;

    /**
     * On exception.
     *
     * @param session the session
     * @param e the e
     */
    void onException(MycatSession session, Exception e);
  }

}
