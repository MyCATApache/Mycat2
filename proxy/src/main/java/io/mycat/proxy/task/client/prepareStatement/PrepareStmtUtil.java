package io.mycat.proxy.task.client.prepareStatement;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.resultset.ResultSetTask;
import java.io.IOException;
import java.util.Objects;

/**
 * @author jamie12221 chen junwen
 * @date 2019-05-10 21:13
 * 预处理命令帮助类
 **/
public interface PrepareStmtUtil {

  CloseTask CLOSE = new CloseTask();
  FetchTask FETCH = new FetchTask();
  ResetTask RESET = new ResetTask();

  /**
   * 该命令清除mysql server中预处理的填充参数
   */
  static void reset(
      MySQLClientSession mysql, long statementId, AsyncTaskCallBack<MySQLClientSession> callBack) {
    RESET.request(mysql, statementId, callBack);
  }

  /**
   * 该命令关闭mysql server中预处理语句句柄
   * @param mysql
   * @param statementId
   * @param callBack
   */
  static void close(
      MySQLClientSession mysql, long statementId, AsyncTaskCallBack<MySQLClientSession> callBack) {
    CLOSE.request(mysql, statementId, callBack);
  }

  /**
   * 游标
   * @param mysql
   * @param stmtId
   * @param numRows
   * @param callBack
   */
  static void fetch(
      MySQLClientSession mysql, long stmtId, long numRows,
      AsyncTaskCallBack<MySQLClientSession> callBack) {
    FETCH.request(mysql, stmtId, numRows, callBack);
  }

  class ResetTask implements ResultSetTask {

    public void request(
        MySQLClientSession mysql, long statementId, AsyncTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, 0x1a, statementId, callBack);
    }
  }

  class CloseTask implements ResultSetTask {

    public void request(
        MySQLClientSession mysql, long statementId, AsyncTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, 0x19, statementId, callBack);
    }

    @Override
    public void onWriteFinished(MySQLClientSession mysql) throws IOException {
      clearAndFinished(mysql, true, null);
    }
  }

  class FetchTask implements ResultSetTask {

    public void request(MySQLClientSession mysql, long stmtId, long numRows,
        AsyncTaskCallBack<MySQLClientSession> callBack) {
      request(mysql, stmtId, numRows, (MycatReactorThread) Thread.currentThread(), callBack);
    }

    public void request(MySQLClientSession mysql, long stmtId, long numRows,
        MycatReactorThread curThread, AsyncTaskCallBack<MySQLClientSession> callBack) {
      Objects.requireNonNull(mysql);
      Objects.requireNonNull(callBack);
      assert mysql.currentProxyBuffer() == null;
      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(curThread.getBufPool()));
      MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(9);
      mySQLPacket.writeByte(MySQLCommandType.COM_STMT_FETCH);
      mySQLPacket.writeFixInt(2, stmtId);
      mySQLPacket.writeFixInt(2, numRows);
      try {
        mysql.setCallBack(callBack);
        mysql.switchNioHandler(this);
        mysql.prepareReveiceResponse();
        mysql.writeCurrentProxyPacket(mySQLPacket, mysql.setPacketId(0));
      } catch (Exception e) {
        this.clearAndFinished(mysql, false, mysql.setLastMessage(e));
      }
    }
  }


}
