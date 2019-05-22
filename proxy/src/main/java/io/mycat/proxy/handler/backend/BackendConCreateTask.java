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
package io.mycat.proxy.handler.backend;

import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.HandshakePacket;
import io.mycat.config.GlobalConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.callback.CommandCallBack;
import io.mycat.proxy.handler.BackendNIOHandler;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPacketResolver;
import io.mycat.proxy.packet.MySQLPayloadType;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 * @date 2019-05-10 22:24 向mysql服务器创建连接
 **/
public final class BackendConCreateTask implements BackendNIOHandler<MySQLClientSession> {

  protected final static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
  final MySQLDatasource datasource;
  final CommandCallBack callback;
  boolean welcomePkgReceived = false;

  public BackendConCreateTask(MySQLDatasource datasource, MySQLSessionManager sessionManager,
      MycatReactorThread curThread, CommandCallBack callback) {
    Objects.requireNonNull(datasource);
    Objects.requireNonNull(sessionManager);
    Objects.requireNonNull(callback);
    this.datasource = datasource;
    this.callback = callback;
    MySQLClientSession mysql = new MySQLClientSession(datasource, this, sessionManager);
    mysql.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
    SocketChannel channel = null;
    try {
      channel = SocketChannel.open();
      channel.configureBlocking(false);
      mysql.register(curThread.getSelector(), channel, SelectionKey.OP_CONNECT);
      channel.connect(new InetSocketAddress(datasource.getIp(), datasource.getPort()));
    } catch (IOException e) {
      onClear(mysql);
      mysql.close(false, e);
      callback.onFinishedException(null, e, null);
      return;
    }
  }

  @Override
  public void onConnect(SelectionKey curKey, MySQLClientSession mysql, boolean success,
      Exception e) {
    if (success) {
      mysql.change2ReadOpts();
    } else {
      onClear(mysql);
      mysql.close(false, e);
      callback.onFinishedException(e, this, null);
    }
  }

  @Override
  public void onSocketRead(MySQLClientSession mysql) {
    try {
      ProxyBuffer proxyBuffer = mysql.currentProxyBuffer().newBufferIfNeed();
      assert (this == mysql.getCurNIOHandler());
      if (!mysql.readFromChannel()) {
        return;
      }
      if (!mysql.readProxyPayloadFully()) {
        return;
      }
      if (!welcomePkgReceived) {
        MySQLPacketResolver packetResolver = mysql.getPacketResolver();
        int serverCapabilities = GlobalConfig.getClientCapabilityFlags().value;
        packetResolver.setCapabilityFlags(serverCapabilities);
        MySQLPacket payload = mysql.currentProxyPayload();
        if (payload.isErrorPacket()) {
          ErrorPacketImpl errorPacket = new ErrorPacketImpl();
          errorPacket.readPayload(payload);
          String errorMessage = new String(errorPacket.getErrorMessage());
          mysql.setLastMessage(errorMessage);
          onClear(mysql);
          mysql.close(false, errorMessage);
          callback.onFinishedErrorPacket(errorPacket, mysql.getPacketResolver().getServerStatus(),
              mysql, this, null);
          return;
        }

        payload.getByte(payload.packetReadStartIndex());
        HandshakePacket hs = new HandshakePacket();
        hs.readPayload(payload);
        mysql.resetCurrentProxyPayload();
        int charsetIndex = hs.getCharacterSet();
        AuthPacket packet = new AuthPacket();
        packet.setCapabilities(serverCapabilities);
        packet.setMaxPacketSize(ProxyRuntime.INSTANCE.getMaxAllowedPacket());
        packet.setCharacterSet((byte) charsetIndex);
        packet.setUsername(datasource.getUsername());
        packet.setPassword(MysqlNativePasswordPluginUtil.scramble411(datasource.getPassword(),
            hs.getAuthPluginDataPartOne() + hs.getAuthPluginDataPartTwo()));
        packet.setAuthPluginName(MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME);
        MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(1024);
        packet.writePayload(mySQLPacket);
        welcomePkgReceived = true;
        mysql.writeCurrentProxyPacket(mySQLPacket, 1);
      } else {
        if (mysql.getPayloadType() == MySQLPayloadType.FIRST_OK) {
          onClear(mysql);
          callback.onFinishedOk(mysql.getPacketResolver().getServerStatus(), mysql, null, null);
          return;
        } else {
          MySQLPacket mySQLPacket = mysql.currentProxyPayload();
          ErrorPacketImpl errorPacket = new ErrorPacketImpl();
          errorPacket.readPayload(mySQLPacket);
          mysql.resetCurrentProxyPayload();
          onClear(mysql);
          mysql.close(false, new String(errorPacket.getErrorMessage()));
          callback.onFinishedErrorPacket(errorPacket, mysql.getPacketResolver().getServerStatus(),
              mysql, this, null);
          return;
        }
      }
    } catch (Exception e) {
      onClear(mysql);
      mysql.close(false, e);
      callback.onFinishedException(e, this, null);
    }
  }

  @Override
  public void onSocketWrite(MySQLClientSession session) {
    try {
      session.writeToChannel();
    } catch (Exception e) {
      onClear(session);
      session.close(false, e);
      callback.onFinishedException(e, this, null);
    }
  }

  @Override
  public void onWriteFinished(MySQLClientSession mysql) {
    mysql.change2ReadOpts();
  }

  public void onClear(MySQLClientSession session) {
    session.resetPacket();
    session.setCallBack(null);
  }

}
