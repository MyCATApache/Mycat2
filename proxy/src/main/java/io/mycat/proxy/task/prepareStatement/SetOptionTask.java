package io.mycat.proxy.task.prepareStatement;
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

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLSetOption;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-01 15:27
 **/
public class SetOptionTask implements ResultSetTask {
  public void request(
      MySQLClientSession mysql, MySQLSetOption setOption, AsynTaskCallBack<MySQLClientSession> callBack){
    request(mysql,setOption,(MycatReactorThread) Thread.currentThread(),callBack);
  }
  public void request(MySQLClientSession mysql, MySQLSetOption setOption,
      MycatReactorThread curThread, AsynTaskCallBack<MySQLClientSession> callBack) {
    try {
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      if (mysql.currentProxyBuffer() != null) {
//                throw new MycatExpection("");
        mysql.currentProxyBuffer().reset();
      }
      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
      MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(7);
      mySQLPacket.writeByte((byte) MySQLCommandType.COM_SET_OPTION);
      mySQLPacket.writeFixInt(2,setOption.getValue());
      mysql.prepareReveiceResponse();
      mysql.writeProxyPacket(mySQLPacket, mysql.setPacketId(0));
    } catch (IOException e) {
      this.clearAndFinished(mysql,false, e.getMessage());
    }
  }

}
