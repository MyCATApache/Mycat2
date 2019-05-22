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
package io.mycat.proxy.handler.front;

import static io.mycat.beans.mysql.MySQLErrorCode.ER_ACCESS_DENIED_ERROR;
import static io.mycat.beans.mysql.MySQLErrorCode.ER_DBACCESS_DENIED_ERROR;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLVersion;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.HandshakePacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.MycatHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import io.mycat.security.MycatSecurityConfig;
import io.mycat.security.MycatUser;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import io.mycat.util.StringUtil;
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
  public void onSocketRead(MycatSession mycat) {
    try {
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
      byte[] input = auth.getPassword();
      int maxPacketSize = auth.getMaxPacketSize();
      String database = auth.getDatabase();

      int capabilities = auth.getCapabilities();
      int characterSet = auth.getCharacterSet();
      Map<String, String> attrs = auth.getClientConnectAttrs();
      String authPluginName = auth.getAuthPluginName();

      MycatSecurityConfig securityManager = ProxyRuntime.INSTANCE.getSecurityManager();

      if (!securityManager.isIgnorePassword()) {
        String password = ProxyRuntime.INSTANCE.getSecurityManager()
                              .getPasswordByUserName(username);
        if (!checkPassword(password, input)) {
          String message = "password is wrong!";
          failture(mycat, message);
          return;
        }
      }
      MycatUser user = securityManager
                           .getUser(mycat.channel().socket().getRemoteSocketAddress().toString(),
                               username, maxPacketSize);

      switch (securityManager.checkSchema(user, database)) {
        case ER_DBACCESS_DENIED_ERROR: {
          String message =
              "Access denied for user '" + username + "' to database '" + database + "'";
          failture(mycat, message);
          mycat.lazyClose(true, message);
          return;
        }
        default: {
          mycat.setUser(user);
          if (!StringUtil.isEmpty(database)) {
            mycat.setSchema(ProxyRuntime.INSTANCE.getSchemaBySchemaName(database));
          }
        }
      }

      if (MySQLServerCapabilityFlags.isCanUseCompressionProtocol(capabilities)) {
        String message = "Can Not Use Compression Protocol!";
        failture(mycat, message);
        mycat.lazyClose(true, message);
        return;
      }

      mycat.setSchema(ProxyRuntime.INSTANCE.getSchemaBySchemaName(database));
      mycat.setServerCapabilities(auth.getCapabilities());
      mycat.setAutoCommit(MySQLAutoCommit.ON);
      mycat.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
      mycat.setCharset(characterSet);
      finished = true;

      mycat.writeOkEndPacket();
    } catch (Exception e) {
      onClear(mycat);
      failture(mycat, e);
    }
  }

  public void failture(MycatSession mycat, String message) {
    mycat.setLastMessage(message);
    mycat.writeErrorEndPacketBySyncInProcessError(2, ER_ACCESS_DENIED_ERROR);
  }

  public void failture(MycatSession mycat, Exception e) {
    mycat.setLastMessage(e);
    mycat.writeErrorEndPacketBySyncInProcessError(2, ER_ACCESS_DENIED_ERROR);
  }

  @Override
  public void onSocketWrite(MycatSession mycat) {
    try {
      mycat.writeToChannel();
    } catch (Exception e) {
      e.printStackTrace();
      onClear(mycat);
      mycat.close(false, e);
    }
  }

  @Override
  public void onWriteFinished(MycatSession mycat) {
    if (!finished) {
      mycat.currentProxyBuffer().reset();
      mycat.change2ReadOpts();
    } else {
      onClear(mycat);
      mycat.switchNioHandler(MycatHandler.INSTANCE);
    }
  }

  public void onClear(MycatSession session) {
    session.onHandlerFinishedClear();
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


  private boolean checkPassword(String rightPassword, byte[] password) {
    if (rightPassword == null || rightPassword.length() == 0) {
      return (password == null || password.length == 0);
    }
    if (password == null || password.length == 0) {
      return false;
    }
    byte[] encryptPass = MysqlNativePasswordPluginUtil.scramble411(rightPassword, seed);
    if (encryptPass != null && (encryptPass.length == password.length)) {
      int i = encryptPass.length;
      while (i-- != 0) {
        if (encryptPass[i] != password[i]) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }
}
