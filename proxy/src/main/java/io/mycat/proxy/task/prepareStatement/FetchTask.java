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
package io.mycat.proxy.task.prepareStatement;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-01 15:27
 **/
public class FetchTask implements ResultSetTask {

  public void request(MySQLSession mysql, long stmtId, long numRows,
      AsynTaskCallBack<MySQLSession> callBack) {
    request(mysql, stmtId, numRows, (MycatReactorThread) Thread.currentThread(), callBack);
  }

  public void request(MySQLSession mysql, long stmtId, long numRows,
      MycatReactorThread curThread, AsynTaskCallBack<MySQLSession> callBack) {
    try {
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      if (mysql.currentProxyBuffer() != null) {
//                throw new MycatExpection("");
        mysql.currentProxyBuffer().reset();
      }
      mysql.setProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
      MySQLPacket mySQLPacket = mysql.newCurrentMySQLPacket();
      mySQLPacket.writeByte((byte) MySQLCommandType.COM_STMT_FETCH);
      mySQLPacket.writeFixInt(2, stmtId);
      mySQLPacket.writeFixInt(2, numRows);
      mysql.prepareReveiceResponse();
      mysql.writeMySQLPacket(mySQLPacket, mysql.setPacketId(0));
    } catch (IOException e) {
      this.clearAndFinished(false, e.getMessage());
    }
  }
}
