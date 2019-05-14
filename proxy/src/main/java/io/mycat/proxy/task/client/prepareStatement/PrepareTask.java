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

import io.mycat.beans.mysql.MySQLPStmtBindValueList;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLPreparedStatement;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.PreparedOKPacket;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.packet.ColumnDefPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.resultset.ResultSetTask;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-10 22:24 向mysq server发送Prepare报文
 **/
public class PrepareTask implements ResultSetTask, MySQLPreparedStatement {

  long statementId;
  String prepareStatement;
  int[] parameterTypeList;
  ColumnDefPacket[] columnDefList;
  boolean newParameterBoundFlag = false;
  MySQLPStmtBindValueList valueList;
  Map<Integer, MySQLPayloadWriter> longDataMap;

  public void request(
      MySQLClientSession mysql, String prepareStatement,
      AsyncTaskCallBack<MySQLClientSession> callBack) {
    Objects.requireNonNull(prepareStatement);
    Objects.requireNonNull(mysql);
    Objects.requireNonNull(prepareStatement);
    this.prepareStatement = prepareStatement;
    request(mysql, 0x16, prepareStatement, callBack);
    mysql.prepareReveicePrepareOkResponse();
  }

  @Override
  public void onPrepareOk(PreparedOKPacket packet) {
    statementId = packet.getPreparedOkStatementId();
    int warningCount = packet.getPreparedOkWarningCount();

    columnDefList = new ColumnDefPacket[packet.getPrepareOkColumnsCount()];
    parameterTypeList = new int[packet.getPrepareOkParametersCount()];
    longDataMap = new HashMap<>();
  }

  @Override
  public void onFinished(MySQLClientSession mysql, boolean success, String errorMessage) {
    valueList = new MySQLPStmtBindValueList(this);
    MySQLClientSession currentMySQLSession = mysql;
    AsyncTaskCallBack<MySQLClientSession> callBack = currentMySQLSession.getCallBackAndReset();
    callBack.finished(currentMySQLSession, this, success, this, errorMessage);
  }

  @Override
  public void onPrepareOkParameterDef(MySQLPacket buffer, int startPos, int endPos) {
    ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
    columnDefPacket.read(buffer, startPos, endPos);
    MySQLClientSession sessionCaller = getSessionCaller();
    int prepareOkParametersCount = sessionCaller.getPacketResolver().getPrepareOkParametersCount();
    int length = parameterTypeList.length - 1;
    int index = length - prepareOkParametersCount;
    parameterTypeList[index] = (byte) columnDefPacket.getColumnType();
  }

  @Override
  public void onPrepareOkColumnDef(MySQLPacket buffer, int startPos, int endPos) {
    ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
    columnDefPacket.read(buffer, startPos, endPos);
    MySQLClientSession sessionCaller = getSessionCaller();
    int prepareOkColumnsCount = sessionCaller.getPacketResolver().getPrepareOkColumnsCount();
    columnDefList[columnDefList.length - prepareOkColumnsCount - 1] = columnDefPacket;
  }


  @Override
  public long getStatementId() {
    return statementId;
  }

  @Override
  public int getColumnsNumber() {
    if (columnDefList == null) {
      return 0;
    }
    return columnDefList.length;
  }

  @Override
  public int getParametersNumber() {
    if (parameterTypeList == null) {
      return 0;
    }
    return parameterTypeList.length;
  }

  @Override
  public Map<Integer, MySQLPayloadWriter> getLongDataMap() {
    return longDataMap;
  }


  @Override
  public boolean setNewParameterBoundFlag(boolean b) {
    return this.newParameterBoundFlag = b;
  }

  @Override
  public int[] getParameterTypeList() {
    return parameterTypeList;
  }

  @Override
  public MySQLPStmtBindValueList getBindValueList() {
    return valueList;
  }

  @Override
  public boolean isNewParameterBoundFlag() {
    return newParameterBoundFlag;
  }
}
