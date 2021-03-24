/**
 * Copyright (C) <2021>  <jamie12221>
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
package io.mycat.proxy.handler.backend;

import io.mycat.beans.mysql.packet.EOFPacket;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.PreparedOKPacket;
import io.mycat.MySQLPacketUtil;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;

public class PrepareStmtTask implements ResultSetHandler {

  private final MycatSession mycat;
  private  long mysqlStatementId;
  private final  long mycatStatementId;
  private final String sql;
  private final boolean proxy;
  private int numOdParmas;

  /**
   * Getter for property 'mysqlStatementId'.
   *
   * @return Value for property 'mysqlStatementId'.
   */
  public long getMysqlStatementId() {
    return mysqlStatementId;
  }

  public PrepareStmtTask(MycatSession mycat, long mycatStatementId, String sql, boolean proxy) {
    this.mycat = mycat;
    this.mycatStatementId = mycatStatementId;
    this.sql = sql;
    this.proxy = proxy;
  }

  public void requestPrepareStatement(MySQLClientSession mysql, ResultSetCallBack<MySQLClientSession> callBack) {
    byte[] payload = MySQLPacketUtil.generatePreparePayloadRequest(sql.getBytes());
    byte[] packet = MySQLPacketUtil.generateMySQLPacket(0, payload);
    request(mysql, packet, callBack);
    mysql.prepareReveicePrepareOkResponse();
  }

  @Override
  public void onPrepareOk(PreparedOKPacket preparedOKPacket) {
    this.mysqlStatementId = preparedOKPacket.getPreparedOkStatementId();
    this.numOdParmas = preparedOKPacket.getPrepareOkParametersCount();
    preparedOKPacket.setPreparedOkStatementId(mycatStatementId);
    if (proxy) {
      byte[] payload = MySQLPacketUtil.generatePrepareOk(preparedOKPacket);
      mycat.writeBytes(payload,false);
    }
  }

  @Override
  public void onPrepareOkParameterDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
    if (proxy) {
      byte[] payload = mySQLPacket.getBytes(startPos, endPos);
      mycat.writeBytes(payload,false);
    }
  }

  @Override
  public void onPrepareOkColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
    if (proxy) {
      byte[] payload = mySQLPacket.getBytes(startPos, endPos);
      mycat.writeBytes(payload,false);
    }
  }

  @Override
  public void onPrepareOkColumnDefEof(EOFPacket packet) {
    if (proxy) {
      byte[] payload = MySQLPacketUtil
          .generateEof(packet.getWarningCount(), packet.getServerStatus());
      mycat.writeBytes(payload,false);
    }
  }

  @Override
  public void onPrepareOkParameterDefEof(EOFPacket packet) {
    if (proxy) {
      byte[] payload = MySQLPacketUtil
          .generateEof(packet.getWarningCount(), packet.getServerStatus());
      mycat.writeBytes(payload,false);
    }
  }

  @Override
  public void onFirstError(ErrorPacketImpl packet) {
    if (proxy) {
      mycat.setLastMessage(packet.getErrorMessageString());
      mycat.setLastErrorCode(packet.getErrorCode());
      mycat.writeErrorEndPacketBySyncInProcessError();
    }
  }

  @Override
  public void onFinishedCollect(MySQLClientSession mysql) {
    if (proxy) {
      //mycat.setResponseFinished(ProcessState.DONE);
    }
  }

  /**
   * Getter for property 'numOdParmas'.
   *
   * @return Value for property 'numOdParmas'.
   */
  public int getNumOdParmas() {
    return numOdParmas;
  }
}