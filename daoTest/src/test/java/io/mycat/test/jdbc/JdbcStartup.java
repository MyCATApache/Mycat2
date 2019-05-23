package io.mycat.test.jdbc;

import io.mycat.MycatProxyBeanProviders;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/**
 * @author jamie12221
 * @date 2019-05-19 18:23
 **/
public class JdbcStartup extends JdbcDao {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";

  @Test
  public void startUp() throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, MycatProxyBeanProviders.INSTANCE, new MycatMonitorCallback() {
      @Override
      public void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

      }
    }, new AsyncTaskCallBack() {
      @Override
      public void onFinished(Object sender, Object future, Object attr) {
        int count = 100;
        AsyncTaskCallBackCounter callBackCounter = new AsyncTaskCallBackCounter(count,
            new AsyncTaskCallBack() {
              @Override
              public void onFinished(Object sender, Object result, Object attr) {
                compelete(future);
              }

              @Override
              public void onException(Exception e, Object sender, Object attr) {

              }
            });
        for (int i = 0; i < count; i++) {
          int index = i;
          new Thread(() -> {
            try {
              Thread.sleep(50);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            try (Connection connection = getConnection()) {
              System.out.println("connectId:" + index);
              for (int j = 0; j < 2; j++) {
                System.out.println("per:" + j);
                try (
                    Statement statement = connection.createStatement()
                ) {
                  statement.execute("select 1");
                }
//                connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
//                connection.setAutoCommit(true);
//                connection.createStatement().execute("select 1");
//                connection.commit();
              }

            } catch (Exception e) {
              e.printStackTrace();
              callBackCounter.onCountFail();
              return;
            }
            callBackCounter.onCountSuccess();
          }).start();

        }
      }

      @Override
      public void onException(Exception e, Object sender, Object attr) {

      }
    });
  }

}
