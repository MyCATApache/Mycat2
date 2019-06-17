package io.mycat.proxy.handler.backend;

import io.mycat.beans.mysql.packet.EOFPacket;
import io.mycat.beans.mysql.packet.PreparedOKPacket;
import io.mycat.command.prepareStatement.PrepareStmtProxy;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.util.ArrayList;

public class PrepareStmtTask implements ResultSetHandler {

  final MycatSession mycat;
  long statementId;
  ArrayList<byte[]> payloadList = new ArrayList<>();
  PrepareStmtProxy prepareStmtProxy;
  String sql;
  /**
   * Getter for property 'statementId'.
   *
   * @return Value for property 'statementId'.
   */
  public long getStatementId() {
    return statementId;
  }

  public PrepareStmtTask(MycatSession mycat,PrepareStmtProxy prepareStmtProxy) {
    this.mycat = mycat;
    this.prepareStmtProxy = prepareStmtProxy;
  }

  public void requestPrepareStatement(MySQLClientSession mysql, String sql,
      ResultSetCallBack<MySQLClientSession> callBack) {
    this.sql = sql;
    byte[] payload = MySQLPacketUtil.generatePreparePayloadRequest(sql.getBytes());
    byte[] packet = MySQLPacketUtil.generateMySQLPacket(0, payload);
    request(mysql, packet, callBack);
    mysql.prepareReveicePrepareOkResponse();
  }

  @Override
  public void onPrepareOk(PreparedOKPacket preparedOKPacket) {
    this.statementId = preparedOKPacket.getPreparedOkStatementId();
    byte[] payload = MySQLPacketUtil.generatePrepareOk(preparedOKPacket);
    payloadList.add(payload);
    mycat.writeBytes(payload);
  }

  @Override
  public void onPrepareOkParameterDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
    byte[] payload = mySQLPacket.getBytes(startPos, endPos);
    payloadList.add(payload);
    mycat.writeBytes(payload);
  }

  @Override
  public void onPrepareOkColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
    byte[] payload = mySQLPacket.getBytes(startPos, endPos);
    payloadList.add(payload);
    mycat.writeBytes(payload);
  }

  @Override
  public void onPrepareOkColumnDefEof(EOFPacket packet) {
    byte[] payload = MySQLPacketUtil
        .generateEof(packet.getWarningCount(), packet.getServerStatus());
    payloadList.add(payload);
    mycat.writeBytes(payload);
  }

  @Override
  public void onPrepareOkParameterDefEof(EOFPacket packet) {
    byte[] payload = MySQLPacketUtil
        .generateEof(packet.getWarningCount(), packet.getServerStatus());
    payloadList.add(payload);
    mycat.writeBytes(payload);
  }

  @Override
  public void onFirstError(ErrorPacketImpl packet) {
    mycat.writeErrorEndPacket(packet);
  }

  @Override
  public void onFinishedCollect(MySQLClientSession mysql) {
    this.prepareStmtProxy.recordPrepareResponse(sql,payloadList);
    mycat.writeEnd();
  }
}