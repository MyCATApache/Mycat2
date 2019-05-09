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

import io.mycat.MySQLAPI;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.ComQueryState;
import io.mycat.replica.Datasource;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class MySQLClientSession extends AbstractMySQLClientSession implements MySQLAPI {

  public MySQLClientSession(Datasource datasource, Selector selector, SocketChannel channel,
      int socketOpt, NIOHandler nioHandler, MySQLSessionManager sessionManager) throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
    this.datasource = datasource;
  }

  private MycatSession mycat;
  private final Datasource datasource;
  private MycatDataNode dataNode;
  private MySQLSessionMonopolizeType monopolizeType = MySQLSessionMonopolizeType.NONE;


  public MycatDataNode getDataNode() {
    return dataNode;
  }

  public void setDataNode(MycatDataNode dataNode) {
    this.dataNode = dataNode;
  }

  public Datasource getDatasource() {
    return datasource;
  }

  public MycatSession getMycatSession() {
    return mycat;
  }


  public void prepareReveiceResponse() {
    this.packetResolver.setState(ComQueryState.FIRST_PACKET);
  }

  public void prepareReveicePrepareOkResponse() {
    this.packetResolver.setState(ComQueryState.FIRST_PACKET);
    this.packetResolver.setCurrentComQuerySQLType(0x22);
  }

  public void bind(MycatSession mycatSession) {
    this.mycat = mycatSession;
    mycatSession.bind(this);
  }

  public boolean unbindMycatIfNeed(MycatSession mycat) {
    if (isMonopolized()) {
      System.out.println("can not unbind monopolized mysql Session");
      return false;
    }
    this.resetPacket();
    this.proxyBuffer = null;
    mycat.resetPacket();
    MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
    reactorThread.getMySQLSessionManager().addIdleSession(this);
    mycat.bind(null);
    mycat.switchWriteHandler(MySQLServerSession.WriteHandler.INSTANCE);
    return true;
  }


  public boolean isMonopolizedByTransaction() {
    int serverStatus = getPacketResolver().getServerStatus();
    return MySQLServerStatusFlags.statusCheck(serverStatus, MySQLServerStatusFlags.IN_TRANSACTION);
  }

  public boolean end() {
    this.resetPacket();
    this.proxyBuffer = null;
    if (mycat != null) {
      mycat.resetPacket();
    }
    MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
    reactorThread.getMySQLSessionManager().addIdleSession(this);
    return true;
  }

  @Override
  public MySQLClientSession getThis() {
    return this;
  }


  public MySQLSessionMonopolizeType getMonopolizeType() {
    return monopolizeType;
  }

  public boolean isMonopolized() {
    MySQLSessionMonopolizeType monopolizeType = getMonopolizeType();
    if (MySQLSessionMonopolizeType.PREPARE_STATEMENT_EXECUTE == monopolizeType ||
            MySQLSessionMonopolizeType.LOAD_DATA == monopolizeType
    ) {
      return true;
    }
    int serverStatus = getPacketResolver().getServerStatus();
    if (
        MySQLServerStatusFlags
            .statusCheck(serverStatus, MySQLServerStatusFlags.IN_TRANSACTION)) {
      setMonopolizeType(MySQLSessionMonopolizeType.TRANSACTION);
      return true;
    } else if (MySQLServerStatusFlags
                   .statusCheck(serverStatus, MySQLServerStatusFlags.CURSOR_EXISTS)) {
      setMonopolizeType(MySQLSessionMonopolizeType.CURSOR_EXISTS);
      return true;
    } else {
      setMonopolizeType(MySQLSessionMonopolizeType.NONE);
      return false;
    }
  }

  public boolean isMonopolizedByPrepareStatement() {
    return getMonopolizeType() == MySQLSessionMonopolizeType.PREPARE_STATEMENT_EXECUTE;
  }

  public boolean isMonopolizedByLoadData() {
    return getMonopolizeType() == MySQLSessionMonopolizeType.LOAD_DATA;
  }

  public void setMonopolizeType(MySQLSessionMonopolizeType monopolizeType) {
    this.monopolizeType = monopolizeType;
  }


  @Override
  public void setCurrentProxyBuffer(ProxyBuffer buffer) {
    this.proxyBuffer = buffer;
  }

  @Override
  public void writeFinished(Session session) throws IOException {
    session.getCurNIOHandler().onWriteFinished(session);
  }
}
