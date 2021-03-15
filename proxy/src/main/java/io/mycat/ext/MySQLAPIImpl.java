/**
 * Copyright (C) <2020>  <chenjunwen>
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
package io.mycat.ext;

import io.mycat.api.MySQLAPI;
import io.mycat.api.callback.MySQLAPIExceptionCallback;
import io.mycat.api.collector.ResultSetCollector;
import io.mycat.api.collector.TextResultSetTransforCollector;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;

public class MySQLAPIImpl implements MySQLAPI {

  final MySQLClientSession mySQLClientSession;

  public MySQLAPIImpl(MySQLClientSession mySQLClientSession) {
    this.mySQLClientSession = mySQLClientSession;
  }

  @Override
  public void query(String sql, ResultSetCollector collector,
      MySQLAPIExceptionCallback exceptionCollector) {
    TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(
        collector);
    TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor);
    queryResultSetTask.request(mySQLClientSession, MySQLCommandType.COM_QUERY, sql.getBytes(),
        new ResultSetCallBack<MySQLClientSession>() {
          @Override
          public void onFinishedSendException(Exception exception, Object sender,
              Object attr) {
            exceptionCollector.onException(exception, MySQLAPIImpl.this);
          }

          @Override
          public void onFinishedException(Exception exception, Object sender, Object attr) {
            exceptionCollector.onException(exception, MySQLAPIImpl.this);
          }

          @Override
          public void onFinished(boolean monopolize, MySQLClientSession mysql,
              Object sender, Object attr) {
            exceptionCollector.onFinished(monopolize, MySQLAPIImpl.this);
          }

          @Override
          public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
              MySQLClientSession mysql, Object sender, Object attr) {
            exceptionCollector.onErrorPacket(errorPacket, monopolize, MySQLAPIImpl.this);
          }
        });
  }

  @Override
  public synchronized void close() {
      mySQLClientSession.close(true,"close");
  }

}