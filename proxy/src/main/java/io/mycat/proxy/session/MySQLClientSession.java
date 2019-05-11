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
import io.mycat.proxy.MySQLPacketExchanger.MySQLProxyNIOHandler;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.replica.MySQLDatasource;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class MySQLClientSession extends
    AbstractSession<MySQLClientSession> implements MySQLProxySession<MySQLClientSession>, MySQLAPI {

  protected final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(this);
  /**
   * mysql session的源配置信息
   */
  private final MySQLDatasource datasource;
  protected ProxyBuffer proxyBuffer;
  protected AsynTaskCallBack<MySQLClientSession> callBack;
  private MycatDataNode dataNode;


  public MycatDataNode getDataNode() {
    return dataNode;
  }

  public void setDataNode(MycatDataNode dataNode) {
    this.dataNode = dataNode;
  }

  public MySQLDatasource getDatasource() {
    return datasource;
  }

  public MycatSession getMycatSession() {
    return mycat;
  }


  public void prepareReveiceResponse() {
    this.packetResolver.prepareReveiceResponse();
  }

  public void prepareReveicePrepareOkResponse() {
    this.packetResolver.prepareReveicePrepareOkResponse();
  }

  public void bind(MycatSession mycatSession) {
    this.mycat = mycatSession;
    mycatSession.bind(this);
  }

  /**
   * 与mycat session绑定的信息 monopolizeType 是无法解绑的原因 TRANSACTION,事务 LOAD_DATA,交换过程
   * PREPARE_STATEMENT_EXECUTE,预处理过程 CURSOR_EXISTS 游标 以上四种情况 mysql客户端的并没有结束对mysql的交互,所以无法解绑
   */
  private MySQLSessionMonopolizeType monopolizeType = MySQLSessionMonopolizeType.NONE;
  /**
   * 绑定的mycat 与同步的dataNode mycat的解绑 mycat = null即可
   */
  private MycatSession mycat;

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
  public void switchMySQLProxy() {
    switchNioHandler(MySQLProxyNIOHandler.INSTANCE);
  }

  public MySQLClientSession(MySQLDatasource datasource, Selector selector, SocketChannel channel,
      int socketOpt,
      NIOHandler nioHandler, SessionManager<MySQLClientSession> sessionManager
  ) throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
    this.datasource = datasource;
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
    switchNioHandler(null);
    this.mycat = null;
    reactorThread.getMySQLSessionManager().addIdleSession(this);
    mycat.bind(null);
    mycat.switchWriteHandler(MySQLServerSession.WriteHandler.INSTANCE);
    return true;
  }

  public boolean end() {
    this.resetPacket();
    this.proxyBuffer = null;
    switchNioHandler(null);
    if (mycat != null) {
      mycat.resetPacket();
    }
    MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
    reactorThread.getMySQLSessionManager().addIdleSession(this);
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MySQLClientSession that = (MySQLClientSession) o;
    return sessionId == that.sessionId;
  }

  @Override
  public int hashCode() {
    int result = mycat != null ? mycat.hashCode() : 0;
    result = 31 * result + (datasource != null ? datasource.hashCode() : 0);
    result = 31 * result + (dataNode != null ? dataNode.hashCode() : 0);
    result = 31 * result + (monopolizeType != null ? monopolizeType.hashCode() : 0);
    return result;
  }

  public byte setPacketId(int packetId) {
    return (byte) this.packetResolver.setPacketId(packetId);
  }

  public byte getPacketId() {
    return (byte) this.packetResolver.getPacketId();
  }

  public byte incrementPacketIdAndGet() {
    return (byte) this.packetResolver.incrementPacketIdAndGet();
  }

  public AsynTaskCallBack<MySQLClientSession> getCallBackAndReset() {
    AsynTaskCallBack<MySQLClientSession> callBack = this.callBack;
    this.callBack = null;
    return callBack;
  }

  public void setCallBack(AsynTaskCallBack<MySQLClientSession> callBack) {
    this.callBack = callBack;
  }

  public MySQLPacketResolver getPacketResolver() {
    return packetResolver;
  }


  public ProxyBuffer currentProxyBuffer() {
    return proxyBuffer;
  }

  public boolean isResponseFinished() {
    return packetResolver.isResponseFinished();
  }

  public void setResponseFinished(boolean b) {
    packetResolver.setRequestFininshed(b);
  }

  public boolean isRequestFinished() {
    return packetResolver.isRequestFininshed();
  }

  public void setRequestFinished(boolean requestFinished) {
    this.packetResolver.setRequestFininshed(requestFinished);
  }


  public MySQLPayloadType getPayloadType() {
    return this.packetResolver.getMySQLPayloadType();
  }

  public boolean isActivated() {
    long timeInterval = System.currentTimeMillis() - this.lastActiveTime;
    return (timeInterval < 60 * 1000);//60 second
  }


  public void checkWriteFinished() throws IOException {
    ProxyBuffer proxyBuffer = currentProxyBuffer();
    if (!proxyBuffer.channelWriteFinished()) {
      this.change2WriteOpts();
    } else {
      writeFinished(this);
    }
  }

  public void resetPacket() {
    packetResolver.reset();
  }

  @Override
  public void close(boolean normal, String hint) {
    try {
      channel.close();
      //proxyBuffer.reset();MySQLSession不能释放Proxybuffer,proxybuffer是mycatSession分配的
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
