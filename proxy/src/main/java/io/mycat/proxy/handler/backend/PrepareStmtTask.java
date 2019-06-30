package io.mycat.proxy.handler.backend;

import io.mycat.beans.mysql.packet.EOFPacket;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.beans.mysql.packet.PreparedOKPacket;
import io.mycat.proxy.MySQLPacketUtil;
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
      mycat.writeErrorEndPacket(packet);
    }
  }

  @Override
  public void onFinishedCollect(MySQLClientSession mysql) {
    if (proxy) {
      mycat.setResponseFinished(true);
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