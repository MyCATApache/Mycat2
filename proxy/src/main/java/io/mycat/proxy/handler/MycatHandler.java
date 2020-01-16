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

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.FrontMySQLPacketResolver;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.ProcessState;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;

/**
 * @author jamie12221
 * The enum Mycat handlerName.
 */
public enum MycatHandler implements NIOHandler<MycatSession> {
  /**
   * PhysicsInstanceImpl mycat handlerName.
   */
  INSTANCE;
  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MycatHandler.class);

  final
  @Override
  public void onSocketRead(MycatSession mycat) {
    try {
//      if (!mycat.checkOpen()) {
//        onException(mycat, new ClosedChannelException());
//        mycat.close(false, "mysql session has closed");
//        return;
//      }
      FrontMySQLPacketResolver resolver = mycat.getMySQLPacketResolver();
       ProcessState processState = mycat.getProcessState();
       if (processState == ProcessState.READY){
         if(resolver.readFromChannel()) {
           mycat.clearReadWriteOpts();
           mycat.handle(resolver.getPayload());
         } else {
           return;
         }
       }


//      mycat.currentProxyBuffer().newBufferIfNeed();
//      if (!mycat.readFromChannel()) {
//        return;
//      }
//      MySQLPacketResolver packetResolver = mycat.getBackendPacketResolver();
//      ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
//      int startIndex = proxyBuffer.channelReadStartIndex();
//      int endPosition = proxyBuffer.channelReadEndIndex();
//      packetResolver.setState(ComQueryState.QUERY_PACKET);
//      while (mycat.readProxyPayloadFully()) {
//        int readPosition = packetResolver.getEndPos();
//        mycat.handle();
//        if (readPosition < endPosition) {
//          proxyBuffer.channelReadEndIndex(endPosition);
//          continue;
//        }
//      }
//      return;
    } catch (ClosedChannelException e) {
      MycatMonitor.onMycatHandlerCloseException(mycat, e);
      onException(mycat, e);
      return;
    } catch (Exception e) {
      MycatMonitor.onMycatHandlerReadException(mycat, e);
      onException(mycat, e);
    }
  }

  @Override
  public void onSocketWrite(MycatSession mycat) {
    try {
      if ((mycat.getChannelKey().interestOps() & SelectionKey.OP_WRITE) != 0) {
        mycat.writeToChannel();
      }
    } catch (Exception e) {
      MycatMonitor.onMycatHandlerWriteException(mycat, e);
      onException(mycat, e);
    }
  }

  @Override
  public void onWriteFinished(MycatSession mycat) {
    try {
      if (mycat.isResponseFinished()) {
        mycat.onHandlerFinishedClear();
      } else {
        mycat.writeToChannel();
      }
    } catch (Exception e) {
      MycatMonitor.onMycatHandlerWriteException(mycat, e);
      onException(mycat, e);
    }
  }

  @Override
  public void onException(MycatSession mycat, Exception e) {
    MycatMonitor.onMycatHandlerException(mycat, e);
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
    MycatMonitor.onMycatHandlerClear(session);
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
