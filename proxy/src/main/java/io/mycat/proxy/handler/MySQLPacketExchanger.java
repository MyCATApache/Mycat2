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

import static io.mycat.logTip.SessionTip.UNKNOWN_IDLE_RESPONSE;

import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public enum MySQLPacketExchanger {
  INSTANCE;
//
//  public boolean handle(MycatSession mycat, boolean noResponse) {
//    mycat.switchWriteHandler(WriteHandler.INSTANCE);
//    ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
//    proxyBuffer.channelWriteStartIndex(0);
//    proxyBuffer.channelWriteEndIndex(proxyBuffer.channelReadEndIndex());
//    MySQLDataNode dataNode = ProxyRuntime.INSTANCE
//                                 .getDataNodeByName(mycat.getDataNode());
//    writeProxyBufferToDataNode(mycat, proxyBuffer, dataNode, noResponse);
//    return false;
//  }
//
//  private void writeProxyBufferToDataNode(
//      MycatSession mycat,
//      ProxyBuffer proxyBuffer,
//      MySQLDataNode dataNode, boolean noResponse) {
//    mycat.getBackend(false, dataNode, null,
//        (mysql, sender, success, result, throwable) -> {
//          if (success) {
//            mycat.clearReadWriteOpts();
//            mysql.setNoResponse(noResponse);
//            mysql.switchProxyNioHandler();
//            try {
//              mysql.writeProxyBufferToChannel(proxyBuffer);
//            } catch (IOException e) {
//              e.printStackTrace();
//              //mycat.closeAllBackendsAndResponseError("");
//            }
//          } else {
//            System.out.println("---------------------------------------------------------");
//            //   mycat.closeAllBackendsAndResponseError("");
//          }
//        });
//  }

  public void onBackendResponse(MySQLClientSession mysql) throws IOException {
    if (!mysql.readFromChannel()) {
      return;
    }
    ProxyBuffer proxyBuffer = mysql.currentProxyBuffer();
    MySQLPacket mySQLPacket = (MySQLPacket) proxyBuffer;
    MySQLPacketResolver packetResolver = mysql.getPacketResolver();
    int startIndex = mySQLPacket.packetReadStartIndex();
    int endPos = startIndex;
    while (mysql.readPartProxyPayload()) {
      endPos = packetResolver.getEndPos();
      mySQLPacket.packetReadStartIndex(endPos);
    }
    proxyBuffer.channelWriteStartIndex(startIndex);
    proxyBuffer.channelWriteEndIndex(endPos);
    mysql.getMycatSession().writeToChannel();
    return;
  }

  public boolean onFrontWriteFinished(MycatSession mycat) {
    MySQLClientSession mysql = mycat.currentBackend();
    if (mysql.isResponseFinished()) {
      mycat.change2ReadOpts();
      mysql.clearReadWriteOpts();
      return true;
    } else {
      mysql.change2ReadOpts();
      mycat.clearReadWriteOpts();
      ProxyBuffer proxyBuffer = mycat.currentProxyBuffer();
      int writeEndIndex = proxyBuffer.channelWriteEndIndex();
      proxyBuffer.channelReadStartIndex(writeEndIndex);
      return false;
    }
  }


  public boolean onBackendWriteFinished(MySQLClientSession mysql) {
    if (!mysql.isNoResponse()) {
      mysql.currentProxyBuffer().reset();
      mysql.currentProxyBuffer().newBuffer();
      mysql.prepareReveiceResponse();
      mysql.change2ReadOpts();
      return false;
    } else {
      return true;
    }
  }

  public enum MySQLProxyNIOHandler implements NIOHandler<MySQLClientSession> {
    INSTANCE;
    protected final static Logger logger = LoggerFactory.getLogger(MySQLProxyNIOHandler.class);
    static final MySQLPacketExchanger HANDLER = MySQLPacketExchanger.INSTANCE;

    @Override
    public void onSocketRead(MySQLClientSession session) throws IOException {
      HANDLER.onBackendResponse(session);
    }

    @Override
    public void onWriteFinished(MySQLClientSession session) {
      boolean b = HANDLER.onBackendWriteFinished(session);
      if (b) {
        MycatSession mycatSession = session.getMycatSession();
        mycatSession.onHandlerFinishedClear(true);
      }
    }

    @Override
    public void onSocketClosed(MySQLClientSession session, boolean normal, String reason) {

    }
  }

    public enum MySQLIdleNIOHandler implements NIOHandler<MySQLClientSession> {
      INSTANCE;
      protected final static Logger logger = LoggerFactory.getLogger(
          MySQLPacketExchanger.MySQLProxyNIOHandler.class);
      @Override
      public void onSocketRead(MySQLClientSession session) throws IOException {
        session.close(false, UNKNOWN_IDLE_RESPONSE.getMessage());
      }

      @Override
      public void onWriteFinished(MySQLClientSession session) throws IOException {
        session.close(false, UNKNOWN_IDLE_RESPONSE.getMessage());
      }
      /**
       * 因为onSocketClosed是被session.close调用,所以不需要重复调用
       */
      @Override
      public void onSocketClosed(MySQLClientSession session, boolean normal, String reason) {

      }
  }

}
