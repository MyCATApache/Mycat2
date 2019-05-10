package io.mycat.proxy.task.client.prepareStatement;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-10 21:13
 **/
public class PrepareStmtUtil {

  public static void reset(
      MySQLClientSession mysql, long statementId, AsynTaskCallBack<MySQLClientSession> callBack) {
    RESET.request(mysql, statementId, callBack);
  }

  public static void close(
      MySQLClientSession mysql, long statementId, AsynTaskCallBack<MySQLClientSession> callBack) {
    CLOSE.request(mysql, statementId, callBack);
  }

  public static void fetch(
      MySQLClientSession mysql, long stmtId, long numRows,
      AsynTaskCallBack<MySQLClientSession> callBack) {
    FETCH.request(mysql, stmtId, numRows, callBack);
  }


  private final static CloseTask CLOSE = new CloseTask();
  private final static FetchTask FETCH = new FetchTask();
  private final static ResetTask RESET = new ResetTask();

  private static class ResetTask implements ResultSetTask {

    public void request(
        MySQLClientSession mysql, long statementId, AsynTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, 0x1a, statementId, callBack);
    }
  }

  private static class CloseTask implements ResultSetTask {

    public void request(
        MySQLClientSession mysql, long statementId, AsynTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, 0x19, statementId, callBack);
    }

    @Override
    public void onWriteFinished(MySQLClientSession mysql) throws IOException {
      clearAndFinished(mysql, true, null);
    }
  }

  private static class FetchTask implements ResultSetTask {

    public void request(MySQLClientSession mysql, long stmtId, long numRows,
        AsynTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, stmtId, numRows, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    public void request(MySQLClientSession mysql, long stmtId, long numRows,
        MycatReactorThread curThread, AsynTaskCallBack<MySQLClientSession> callBack) {
      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
      MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(9);
      mySQLPacket.writeByte((byte) MySQLCommandType.COM_STMT_FETCH);
      mySQLPacket.writeFixInt(2, stmtId);
      mySQLPacket.writeFixInt(2, numRows);
      try {
        mysql.setCallBack(callBack);
        mysql.switchNioHandler(this);
        mysql.prepareReveiceResponse();
        mysql.writeProxyPacket(mySQLPacket, mysql.setPacketId(0));
      } catch (IOException e) {
        this.clearAndFinished(mysql, false, e.getMessage());
      }
    }
  }


}
