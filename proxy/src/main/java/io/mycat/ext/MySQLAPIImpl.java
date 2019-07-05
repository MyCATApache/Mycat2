package io.mycat.ext;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.mysqlapi.MySQLAPI;
import io.mycat.mysqlapi.callback.MySQLAPIExceptionCallback;
import io.mycat.mysqlapi.collector.ResultSetCollector;
import io.mycat.mysqlapi.collector.TextResultSetTransforCollector;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;

public class MySQLAPIImpl implements MySQLAPI {

  final MySQLClientSession mySQLClientSession;

  public MySQLAPIImpl(MySQLClientSession mySQLClientSession) {
    this.mySQLClientSession = mySQLClientSession;
  }

  @Override
  public void query(String sql, ResultSetCollector collector,
      MySQLAPIExceptionCallback exceptionCollector) {
    TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(
        collector);
    TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor);
    queryResultSetTask.request(mySQLClientSession, MySQLCommandType.COM_QUERY, sql.getBytes(),
        new ResultSetCallBack<MySQLClientSession>() {
          @Override
          public void onFinishedSendException(Exception exception, Object sender,
              Object attr) {
            exceptionCollector.onException(exception, MySQLAPIImpl.this);
          }

          @Override
          public void onFinishedException(Exception exception, Object sender, Object attr) {
            exceptionCollector.onException(exception, MySQLAPIImpl.this);
          }

          @Override
          public void onFinished(boolean monopolize, MySQLClientSession mysql,
              Object sender, Object attr) {
            exceptionCollector.onFinished(monopolize, MySQLAPIImpl.this);
          }

          @Override
          public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
              MySQLClientSession mysql, Object sender, Object attr) {
            exceptionCollector.onErrorPacket(errorPacket, monopolize, MySQLAPIImpl.this);
          }
        });
  }

  @Override
  public void close() {
    mySQLClientSession.getSessionManager().addIdleSession(mySQLClientSession);
  }

}