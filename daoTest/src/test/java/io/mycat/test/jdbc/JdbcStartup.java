package io.mycat.test.jdbc;

import io.mycat.MycatProxyBeanProviders;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.test.ModualTest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 * @date 2019-05-19 18:23
 **/
public class JdbcStartup extends ModualTest {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStartup.class);

  @Test
  public void startUp() throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, MycatProxyBeanProviders.INSTANCE, new MycatMonitorCallback() {
      @Override
      public void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

      }
    }, new AsyncTaskCallBack() {
      @Override
      public void onFinished(Object sender, Object future, Object attr) {
        int count = 80;
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
            try (Connection connection = getConnection()) {
              LOGGER.debug("connectId:{}", index);
              for (int j = 0; j < 500; j++) {
                LOGGER.debug("per:{}", j);
                try (
                    Statement statement = connection.createStatement()
                ) {
                  connection.setAutoCommit(false);
                  connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

                  connection.createStatement().execute("select 1");
                  connection.commit();
                }

              }

            } catch (Exception e) {
              LOGGER.error("{}", e);
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
