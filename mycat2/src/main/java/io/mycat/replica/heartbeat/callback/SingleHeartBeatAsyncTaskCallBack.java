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
package io.mycat.replica.heartbeat.callback;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.collector.OneResultSetCollector;
import io.mycat.collector.TextResultSetTransforCollector;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heartbeat.HeartbeatDetector;

/**
 * @author : zhangwy
 *  date Date : 2019年05月15日 21:34
 */
public class SingleHeartBeatAsyncTaskCallBack extends HeartBeatAsyncTaskCallBack {

  final static String sql = "select user()";

  public SingleHeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
    super(heartbeatDetector);
  }

  @Override
  public void onSession(MySQLClientSession session, Object sender, Object attr) {
    if (isQuit == false) {
      OneResultSetCollector collector = new OneResultSetCollector();
      TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(collector);
      TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor);

      queryResultSetTask
          .request(session, MySQLCommandType.COM_QUERY, sql,
              new ResultSetCallBack<MySQLClientSession>() {
                @Override
                public void onFinishedSendException(Exception exception, Object sender,
                    Object attr) {
                  onStatus(DatasourceStatus.ERROR_STATUS);
                }

                @Override
                public void onFinishedException(Exception exception, Object sender, Object attr) {
                  onStatus(DatasourceStatus.ERROR_STATUS);
                }

                @Override
                public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender,
                    Object attr) {
                  onStatus(DatasourceStatus.OK_STATUS);
                  mysql.getSessionManager().addIdleSession(mysql);
                }

                @Override
                public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                    MySQLClientSession mysql, Object sender, Object attr) {

                }
              });
    }
  }

  @Override
  public void onException(Exception exception, Object sender, Object attr) {
    onStatus(DatasourceStatus.ERROR_STATUS);
  }


}
