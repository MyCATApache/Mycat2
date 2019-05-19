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

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.task.client.resultset.ResultSetTask;
import java.util.Collection;

/**
 * @author jamie12221
 * @date 2019-05-09 15:08
 **/
public class MultiMySQLUpdateTask implements ResultSetTask {

  int success = 0;

  public MultiMySQLUpdateTask(MycatSession mycat, byte[] packetData,
      Collection<MySQLDataNode> dataNodeList,
      AsyncTaskCallBack<MycatSession> finalCallBack) {
    final AsyncTaskCallBack<MySQLClientSession> callBack = (session, sender, success, result, attr) -> {
      if (success) {
        session.getSessionManager().addIdleSession(session);
        if (++MultiMySQLUpdateTask.this.success == dataNodeList.size()) {
          finalCallBack.finished(mycat, sender, success, result, attr);
        }
      } else {
        finalCallBack.finished(mycat, sender, success, result, attr);
      }
    };

    MySQLIsolation isolation = mycat.getIsolation();
    MySQLAutoCommit autoCommit = mycat.getAutoCommit();
    String charsetName = mycat.getCharsetName();
    String characterSetResults = mycat.getCharacterSetResults();
    for (MySQLDataNode dataNode : dataNodeList) {
      MySQLTaskUtil
          .getMySQLSession(dataNode, isolation, autoCommit, charsetName, characterSetResults,
              false,
              null, (session, sender, success, result, attr) ->
              {
                if (success) {
                  request(session, packetData, callBack);
                } else {
                  callBack.finished(null, sender, false, result, null);
                }
              });
    }
  }

  @Override
  public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

  }

  @Override
  public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

  }

  @Override
  public void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

  }
}
