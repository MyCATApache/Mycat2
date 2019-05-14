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

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLVersion;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.HandshakePacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 * @date 2019-05-07 13:58
 *
 * 服务器发送给客户端的验证处理器
 **/
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
    AuthPacket auth = new AuthPacket();
    auth.readPayload(mySQLPacket);
    mycat.resetCurrentProxyPayload();

    String username = auth.getUsername();
    byte[] password = auth.getPassword();

    int maxPacketSize = auth.getMaxPacketSize();
    String database = auth.getDatabase();

    int capabilities = auth.getCapabilities();
    int characterSet = auth.getCharacterSet();
    Map<String, String> attrs = auth.getClientConnectAttrs();
    String authPluginName = auth.getAuthPluginName();

    if (MySQLServerCapabilityFlags.isCanUseCompressionProtocol(capabilities)) {
      mycat.setLastMessage("Can Not Use Compression Protocol!");
      mycat.writeErrorEndPacket();
      return;
    }

    mycat.setServerCapabilities(auth.getCapabilities());
    mycat.setAutoCommit(MySQLAutoCommit.ON);
    mycat.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
    int index = characterSet;
    String charset = ProxyRuntime.INSTANCE.getCharsetById(index);
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
      mycat.switchNioHandler(MycatHandler.INSTANCE);
    }
  }

  @Override
  public void onSocketClosed(MycatSession session, boolean normal, String reason) {

  }


  public void sendAuthPackge() {
    byte[][] seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
    this.seed = seedParts[2];
    HandshakePacket hs = new HandshakePacket();
    hs.setProtocolVersion(MySQLVersion.PROTOCOL_VERSION);
    hs.setServerVersion(new String(MySQLVersion.SERVER_VERSION));
    hs.setConnectionId(mycat.sessionId());
    hs.setAuthPluginDataPartOne(new String(seedParts[0]));
    int serverCapabilities = MySQLServerCapabilityFlags.getDefaultServerCapabilities();
    mycat.setServerCapabilities(serverCapabilities);
    hs.setCapabilities(new MySQLServerCapabilityFlags(serverCapabilities));
    hs.setHasPartTwo(true);
    hs.setCharacterSet(8);
    hs.setStatusFlags(2);
    hs.setAuthPluginDataLen(21); // 有插件的话，总长度必是21, seed
    hs.setAuthPluginDataPartTwo(new String(seedParts[1]));
    hs.setAuthPluginName(MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME);
    MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter();
    hs.writePayload(mySQLPayloadWriter);
    mycat.setPakcetId(-1);//使用获取的packetId变为0
    mycat.writeBytes(mySQLPayloadWriter.toByteArray());
  }
}
