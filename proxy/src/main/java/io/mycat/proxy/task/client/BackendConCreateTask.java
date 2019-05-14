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
package io.mycat.proxy.task.client;

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.HandshakePacket;
import io.mycat.config.GlobalConfig;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBuffer;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.MySQLPayloadType;
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
public final class BackendConCreateTask implements NIOHandler<MySQLClientSession> {

  protected final static Logger logger = LoggerFactory.getLogger(BackendConCreateTask.class);
  final MySQLDatasource datasource;
  final AsyncTaskCallBack<MySQLClientSession> callback;
  boolean welcomePkgReceived = false;

  public BackendConCreateTask(MySQLDatasource datasource, MySQLSessionManager sessionManager,
      MycatReactorThread curThread, AsyncTaskCallBack<MySQLClientSession> callback)
      throws IOException {
    Objects.requireNonNull(datasource);
    Objects.requireNonNull(sessionManager);
    Objects.requireNonNull(callback);
    Objects.requireNonNull(callback);
    this.datasource = datasource;
    this.callback = callback;
    SocketChannel channel = SocketChannel.open();
    channel.configureBlocking(false);
    MySQLClientSession mySQLSession = new MySQLClientSession(datasource, curThread.getSelector(),
        channel, SelectionKey.OP_CONNECT, this, sessionManager);
    mySQLSession.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
    channel.connect(new InetSocketAddress(datasource.getIp(), datasource.getPort()));
  }

  @Override
  public void onConnect(SelectionKey curKey, MySQLClientSession mysql, boolean success,
      Throwable throwable) throws IOException {
    if (success) {
      mysql.change2ReadOpts();
    } else {
      String message = mysql.setLastMessage(throwable);
      mysql.resetPacket();
      callback.finished(mysql, this, false, message, null);
    }
  }

  @Override
  public void onSocketRead(MySQLClientSession mysql) throws IOException {
    ProxyBuffer proxyBuffer = mysql.currentProxyBuffer().newBufferIfNeed();
    assert (this == mysql.getCurNIOHandler());
    if (!mysql.readFromChannel()) {
      return;
    }
    if (!mysql.readProxyPayloadFully()) {
      return;
    }
    if (!welcomePkgReceived) {
      int serverCapabilities = GlobalConfig.getClientCapabilityFlags().value;
      mysql.getPacketResolver().setCapabilityFlags(serverCapabilities);

      HandshakePacket hs = new HandshakePacket();
      if ((proxyBuffer.get(4) & 0xff) == 0xff) {
        throw new MycatExpection("receive error packet");
      }
      hs.readPayload(mysql.currentProxyPayload());
      mysql.resetCurrentProxyPayload();
      int charsetIndex = hs.getCharacterSet();
      AuthPacket packet = new AuthPacket();
      packet.setCapabilities(serverCapabilities);
      packet.setMaxPacketSize(1024 * 1000);
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
        mysql.resetPacket();
        callback.finished(mysql, this, true, null, null);
      } else {
        MySQLPacket mySQLPacket = mysql.currentProxyPayload();
        ErrorPacketImpl errorPacket = new ErrorPacketImpl();
        errorPacket.readPayload(mySQLPacket);
        String message = new String(errorPacket.getErrorMessage());
        mysql.resetCurrentProxyPayload();
        callback.finished(mysql, this, false, message, null);
      }
    }

  }

  @Override
  public void onWriteFinished(MySQLClientSession mysql) throws IOException {
    mysql.change2ReadOpts();
  }

  @Override
  public void onSocketClosed(MySQLClientSession session, boolean normal, String reason) {
    callback.finished(session, this, normal, reason, null);
  }


}
