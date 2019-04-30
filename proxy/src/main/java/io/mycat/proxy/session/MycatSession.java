/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.session;

import io.mycat.beans.DataNode;
import io.mycat.beans.Schema;
import io.mycat.beans.mysql.MySQLCapabilities;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MySQLCommand;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.buffer.BufferPool;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.ErrorCode;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.router.ByteArrayView;
import io.mycat.router.RouteResult;
import io.mycat.router.RouteStrategy;
import io.mycat.router.RouteStrategyImpl;
import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class MycatSession extends AbstractMySQLSession {

  public MycatSession(BufferPool bufferPool, Selector selector, SocketChannel channel,
      int socketOpt, NIOHandler nioHandler, SessionManager<? extends Session> sessionManager)
      throws IOException {
    super(selector, channel, socketOpt, nioHandler, sessionManager);
    setCurrentProxyBuffer(new ProxyBufferImpl(bufferPool));
  }

  private MySQLCommand curSQLCommand;
  private MySQLSession backend;

  private RouteStrategy routeStrategy = new RouteStrategyImpl();

  public RouteResult route(ByteArrayView byteArrayView) {
    routeStrategy.preprocessRoute(byteArrayView, this.getSchema().getSchemaName());
    return routeStrategy.getRouteResult();
  }

  public Schema getSchema() {
    return schema == null ? schema = MycatRuntime.INSTANCE.getMycatConfig().getDefaultSchema()
               : schema;
  }


  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  private Schema schema;

  public MySQLSession getBackend() {
    return backend;
  }

  public MySQLCommand getCurSQLCommand() {
    return curSQLCommand;
  }

  public byte getPacketId() {
    return (byte) super.packetResolver.getPacketId();
  }


  public int getAndIncrementPacketId() {
    return super.packetResolver.getAndIncrementPacketId();
  }


  public boolean isRequestFinished() {
    return super.packetResolver.isRequestFininshed();
  }


  public void switchSQLCommand(MySQLCommand newCmd) {
    logger.debug("{} switch command from {} to  {} ", this, this.curSQLCommand, newCmd);
    this.curSQLCommand = newCmd;
  }

  public void closeAllBackendsAndResponseError(String errorMessage) {

  }

  public void getSingleBackendAndCallBack(boolean runOnSlave, DataNode dataNode,
      LoadBalanceStrategy strategy, AsynTaskCallBack<MySQLSession> finallyCallBack) {
    dataNode
        .getMySQLSession(this.getIsolation(), this.getAutoCommit(), this.getCharset(), runOnSlave,
            strategy, (session, sender, success, result, errorMessage) ->
            {
              if (success) {
                session.bind(this);
                finallyCallBack.finished(session, sender, true, result, errorMessage);
              } else {
                finallyCallBack.finished(null, sender, false, result, errorMessage);
              }
            });
  }

  @Override
  public void close(boolean normal, String hint) {

  }


  /**
   * 服务器能力@cjw
   */
  public int getServerCapabilities() {
    int flag = 0;
    flag |= MySQLCapabilities.CLIENT_LONG_PASSWORD;
    flag |= MySQLCapabilities.CLIENT_FOUND_ROWS;
    flag |= MySQLCapabilities.CLIENT_LONG_FLAG;
    flag |= MySQLCapabilities.CLIENT_CONNECT_WITH_DB;
    // flag |= MySQLCapabilities.CLIENT_NO_SCHEMA;
    // boolean usingCompress = MycatServer.getInstance().getConfig()
    // .getSystem().getUseCompression() == 1;
    // if (usingCompress) {
    // flag |= MySQLCapabilities.CLIENT_COMPRESS;
    // }
    flag |= MySQLCapabilities.CLIENT_ODBC;
    flag |= MySQLCapabilities.CLIENT_LOCAL_FILES;
    flag |= MySQLCapabilities.CLIENT_IGNORE_SPACE;
    flag |= MySQLCapabilities.CLIENT_PROTOCOL_41;
    flag |= MySQLCapabilities.CLIENT_INTERACTIVE;
    // flag |= MySQLCapabilities.CLIENT_SSL;
    flag |= MySQLCapabilities.CLIENT_IGNORE_SIGPIPE;
    flag |= MySQLCapabilities.CLIENT_TRANSACTIONS;
    // flag |= ServerDefs.CLIENT_RESERVED;
    flag |= MySQLCapabilities.CLIENT_SECURE_CONNECTION;
    flag |= MySQLCapabilities.CLIENT_PLUGIN_AUTH;
    flag |= MySQLCapabilities.CLIENT_CONNECT_ATTRS;
    return flag;
  }

  public void writeErrorPacket(String message) throws IOException {
    ErrorPacketImpl errorPacket = new ErrorPacketImpl();
    errorPacket.errno = ErrorCode.ER_UNKNOWN_ERROR;
    errorPacket.message = message;
    errorPacket.writePayload(newCurrentMySQLPacket());
    this.packetResolver.setRequestFininshed(true);
    this.writeToChannel();
    this.switchSQLCommand(null);
  }

  public void bind(MySQLSession mySQLSession) {
    this.backend = mySQLSession;
  }
}
