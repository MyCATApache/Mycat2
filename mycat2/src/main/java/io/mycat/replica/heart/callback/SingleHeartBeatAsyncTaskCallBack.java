package io.mycat.replica.heart.callback;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.collector.OneResultSetCollector;
import io.mycat.collector.TextResultSetTransforCollector;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.replica.heart.DatasourceStatus;
import io.mycat.replica.heart.HeartBeatAsyncTaskCallBack;
import io.mycat.replica.heart.HeartbeatDetector;

/**
 * @author : zhangwy
 * @date Date : 2019年05月15日 21:34
 */
public class SingleHeartBeatAsyncTaskCallBack extends HeartBeatAsyncTaskCallBack {

  final static String sql = "select user()";

  public SingleHeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
    super(heartbeatDetector);
  }

  @Override
  public void onSession(MySQLClientSession session, Object sender, Object attr) {
    if (isQuit == false) {
      OneResultSetCollector collector = new OneResultSetCollector();
      TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(collector);
      TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor);

      queryResultSetTask
          .request(session, MySQLCommandType.COM_QUERY, sql,
              new ResultSetCallBack<MySQLClientSession>() {
                @Override
                public void onFinishedSendException(Exception exception, Object sender,
                    Object attr) {
                  onStatus(DatasourceStatus.ERROR_STATUS);
                }

                @Override
                public void onFinishedException(Exception exception, Object sender, Object attr) {
                  onStatus(DatasourceStatus.ERROR_STATUS);
                }

                @Override
                public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender,
                    Object attr) {
                  collector.toString();
                  onStatus(DatasourceStatus.OK_STATUS);
                  mysql.getSessionManager().addIdleSession(mysql);
                }
              });
    }
  }

  @Override
  public void onException(Exception exception, Object sender, Object attr) {
    onStatus(DatasourceStatus.ERROR_STATUS);
  }


}
