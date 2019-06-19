package io.mycat.proxy.handler.backend;

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.session.MySQLClientSession;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLSessionSyncTask {
  protected final static Logger LOGGER = LoggerFactory.getLogger(
      MySQLSessionSyncTask.class);
  private MySQLClientSession mysql;
  private Object sender;
  MySQLSynContext context;
  SessionSyncCallback callBack;

  public MySQLSessionSyncTask(MySQLSynContext context,MySQLClientSession mysql, Object sender,
      SessionSyncCallback callBack) {
    this.context = context;
    this.mysql = mysql;
    this.sender = sender;
    this.callBack = callBack;
  }

  public void run() {
    MySQLDataNode dataNode = context.getDataNode();
    MySQLAutoCommit autoCommit = context.getAutoCommit();
    String characterSetResult = context.getCharacterSetResult();
    String charset = context.getCharset();
    MySQLIsolation isolation = context.getIsolation();
    long sqlSelectLimit = context.getSqlSelectLimit();
    if (dataNode.equals(mysql.getDataNode())) {
      if (autoCommit == mysql.isAutomCommit() &&
          charset.equals(mysql.getCharset()) &&
          isolation.equals(mysql.getIsolation()) && Objects.equals(characterSetResult,
          mysql.getCharacterSetResult())
      ) {
        callBack.onSession(mysql, sender, null);
        return;
      }
    }
    String databaseName = dataNode.getDatabaseName();
    String sql =
        isolation.getCmd() + autoCommit.getCmd() + "USE " + databaseName
            + ";" + "SET names " + charset + ";"
            + ("SET character_set_results =" + (
            characterSetResult == null || "".equals(characterSetResult) ? "NULL"
                : characterSetResult))+";"
        +("SET SQL_SELECT_LIMIT="+((sqlSelectLimit==-1)?"DEFAULT":sqlSelectLimit));
    ResultSetHandler.DEFAULT.request(mysql, MySQLCommandType.COM_QUERY, sql,
        new ResultSetCallBack<MySQLClientSession>() {

          @Override
          public void onFinished(boolean monopolize, MySQLClientSession mysql,
              Object sender, Object attr) {
            mysql.setCharset(charset);
            mysql.setDataNode(dataNode);
            mysql.setIsolation(isolation);
            mysql.setCharacterSetResult(characterSetResult);
            mysql.setSelectLimit(sqlSelectLimit);
            assert autoCommit == mysql.isAutomCommit();
            MycatMonitor.onSynchronizationState(mysql);
            callBack.onSession(mysql, sender, attr);
          }

          @Override
          public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
              MySQLClientSession mysql, Object sender, Object attr) {
            String messageString = errorPacket.getErrorMessageString();
            mysql.close(false, messageString);
            if (monopolize) {
              callBack.onException(new MycatExpection(messageString), this, null);
              return;
            } else {
              callBack.onErrorPacket(errorPacket, false,mysql, this,null);
              return;
            }
          }

          @Override
          public void onFinishedSendException(Exception exception, Object sender,
              Object attr) {
            callBack.onException(exception, sender, attr);
          }

          @Override
          public void onFinishedException(Exception exception, Object sender, Object attr) {
            callBack.onException(exception, sender, attr);
          }

        });
  }
}