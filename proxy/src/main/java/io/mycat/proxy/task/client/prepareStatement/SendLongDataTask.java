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
package io.mycat.proxy.task.client.prepareStatement;

import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLPreparedStatement;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacketUtil;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.resultset.ResultSetTask;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author jamie12221
 * @date 2019-05-10 22:24 向mysq server发送LONG DATA(BLOB)报文
 **/
public class SendLongDataTask implements ResultSetTask {

  MySQLPreparedStatement preparedStatement;

  public void request(MySQLClientSession mysql, MySQLPreparedStatement preparedStatement,
      AsyncTaskCallBack<MySQLClientSession> callBack) {
    Objects.requireNonNull(mysql);
    Objects.requireNonNull(preparedStatement);
    Objects.requireNonNull(callBack);
    assert mysql.currentProxyBuffer() == null;
    MycatReactorThread reactorThread = (MycatReactorThread) Thread.currentThread();
    ProxyBufferImpl proxyBuffer = new ProxyBufferImpl(reactorThread.getBufPool());
    proxyBuffer.newBuffer();
    mysql.setCurrentProxyBuffer(proxyBuffer);
    mysql.setCallBack(callBack);
    mysql.switchNioHandler(this);
    this.preparedStatement = preparedStatement;
    sendData(mysql);
  }

  private void sendData(MySQLClientSession mysql) {
    try {
      Set<Map.Entry<Integer, MySQLPayloadWriter>> entries = this.preparedStatement.getLongDataMap()
                                                                .entrySet();
      Iterator<Map.Entry<Integer, MySQLPayloadWriter>> iterator = entries.iterator();
      if (iterator.hasNext()) {
        Map.Entry<Integer, MySQLPayloadWriter> next = iterator.next();
        try (MySQLPayloadWriter longData = next.getValue()) {
          byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, longData.toByteArray());
          mysql.writeProxyBufferToChannel(bytes);
          iterator.remove();
        }
      } else {
        clearAndFinished(mysql, true, null);
      }
    } catch (Exception e) {
      clearAndFinished(mysql, false, mysql.setLastMessage(e));
    }
  }

  @Override
  public void onWriteFinished(MySQLClientSession mysql) throws IOException {
    sendData(mysql);
  }
}
