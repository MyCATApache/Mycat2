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

import io.mycat.beans.MySQLServerStatus;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.packet.MySQLPacketProcessType;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPacketResolverImpl;
import io.mycat.proxy.task.AsynTaskCallBack;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

public abstract class AbstractMySQLClientSession<T extends AbstractSession<T>> extends
    AbstractSession<T> implements MySQLProxySession<T> {
  public AbstractMySQLClientSession(Selector selector, SocketChannel channel, int socketOpt,
      NIOHandler nioHandler, SessionManager<? extends Session> sessionManager) throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
  }

  final protected MySQLServerStatus serverStatus = new MySQLServerStatus();
  protected ProxyBuffer proxyBuffer;
  protected AsynTaskCallBack<T> callBack;

  public byte setPacketId(int packetId) {
    return (byte) this.packetResolver.setPacketId(packetId);
  }

  public byte getPacketId() {
    return (byte) this.packetResolver.getPacketId();
  }

  public byte incrementPacketIdAndGet() {
    return (byte) this.packetResolver.incrementPacketIdAndGet();
  }

  public AsynTaskCallBack<T> getCallBackAndReset() {
    AsynTaskCallBack<T> callBack = this.callBack;
    this.callBack = null;
    return callBack;
  }


  public void setCallBack(AsynTaskCallBack<T> callBack) {
    this.callBack = callBack;
  }


  public void setCharset(int charsetIndex, String charsetName) {
    serverStatus.setCharset(charsetIndex, charsetName, Charset.forName(charsetName));
  }

  public void setClientUser(String clientUser) {
    this.serverStatus.setClientUser(clientUser);
  }

  public void setAutoCommit(MySQLAutoCommit autoCommit) {
    this.serverStatus.setAutoCommit(autoCommit);
  }

  public MySQLIsolation getIsolation() {
    return this.serverStatus.getIsolation();
  }

  public void setIsolation(MySQLIsolation isolation) {
    this.serverStatus.setIsolation(isolation);
  }

  protected final MySQLPacketResolver packetResolver = new MySQLPacketResolverImpl(this);

  public MySQLPacketResolver getPacketResolver() {
    return packetResolver;
  }


  public ProxyBuffer currentProxyBuffer() {
    return proxyBuffer;
  }


  public String getCharset() {
    return this.serverStatus.getCharsetName();
  }

  public int getCharsetIndex() {
    return this.serverStatus.getCharsetIndex();
  }

  public String getClientUser() {
    return this.serverStatus.getClientUser();
  }

  public MySQLAutoCommit getAutoCommit() {
    return this.serverStatus.getAutoCommit();
  }

  public void setResponseFinished(boolean b) {
    packetResolver.setRequestFininshed(b);
  }

  public boolean isResponseFinished() {
    return packetResolver.isResponseFinished();
  }

  public boolean isRequestFinished() {
    return packetResolver.isRequestFininshed();
  }

  public void setRequestFinished(boolean requestFinished) {
    this.packetResolver.setRequestFininshed(requestFinished);
  }



  public MySQLPacketProcessType getPayloadType() {
    return this.packetResolver.getPailoadType();
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
      writeFinished((T)this);
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
