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

import io.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLCollationIndex;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLVersion;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.MainMycatNIOHandler;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.packet.AuthPacketImpl;
import io.mycat.proxy.packet.HandshakePacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLReplica;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLClientAuthHandler implements NIOHandler<MycatSession> {
  private static final Logger logger = LoggerFactory.getLogger(MySQLClientAuthHandler.class);
  public byte[] seed;
  public MycatSession mycat;
  private boolean finished = false;

  public void setMycatSession(MycatSession mycatSession) {
    this.mycat = mycatSession;
  }

  @Override
  public void onSocketRead(MycatSession mycat) throws IOException {
    mycat.currentProxyBuffer().newBufferIfNeed();
    if (mycat.getCurNIOHandler() != this) {
      return;
    }
    if (!mycat.readFromChannel()) {
      return;
    }
    if (!mycat.readProxyPayloadFully()) {
      return;
    }

    MySQLPacket mySQLPacket = mycat.currentProxyPayload();
    AuthPacketImpl auth = new AuthPacketImpl();
    auth.readPayload(mySQLPacket);
    mycat.resetCurrentProxyPayload();

    mycat.setServerCapabilities(auth.capabilities);
    mycat.setAutoCommit(MySQLAutoCommit.ON);
    MycatSchema defaultSchema = MycatRuntime.INSTANCE.getDefaultSchema();
    mycat.setSchema(defaultSchema);
    mycat.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
    MySQLDataNode dataNode = (MySQLDataNode) MycatRuntime.INSTANCE.getDataNodeByName(
        defaultSchema.getDefaultDataNode());
    MySQLReplica replica = (MySQLReplica) dataNode.getReplica();
    MySQLCollationIndex collationIndex = replica.getCollationIndex();
    int index = auth.characterSet & 0xff;
    String charset = "UTF8";
    mycat.setCharset(index, charset);
    finished = true;

    mycat.writeOkEndPacket();
  }

  @Override
  public void onSocketWrite(MycatSession mysql) throws IOException {
    mysql.writeToChannel();
  }

  @Override
  public void onWriteFinished(MycatSession mycat) throws IOException {
    if (!finished) {
      mycat.currentProxyBuffer().reset();
      mycat.change2ReadOpts();
    } else {
      mycat.resetPacket();
      mycat.switchNioHandler(MainMycatNIOHandler.INSTANCE);
    }
  }

  @Override
  public void onSocketClosed(MycatSession session, boolean normal) {

  }

  public void sendAuthPackge() {
    try {
      byte[][] seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
      this.seed = seedParts[2];
      HandshakePacketImpl hs = new HandshakePacketImpl();
      hs.protocolVersion = MySQLVersion.PROTOCOL_VERSION;
      hs.serverVersion = new String(MySQLVersion.SERVER_VERSION);
      hs.connectionId = mycat.sessionId();
      hs.authPluginDataPartOne = new String(seedParts[0]);
      int serverCapabilities = MySQLServerCapabilityFlags.getDefaultServerCapabilities();
      mycat.setServerCapabilities(serverCapabilities);
      hs.capabilities = new MySQLServerCapabilityFlags(serverCapabilities);
      hs.hasPartTwo = true;
      hs.characterSet = 8;
      hs.statusFlags = 2;
      hs.authPluginDataLen = 21; // 有插件的话，总长度必是21, seed
      hs.authPluginDataPartTwo = new String(seedParts[1]);
      hs.authPluginName = MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME;

      MySQLPacket mySQLPacket = mycat.newCurrentProxyPacket(1024);
      hs.writePayload(mySQLPacket);
      mycat.writeProxyPacket(mySQLPacket);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
