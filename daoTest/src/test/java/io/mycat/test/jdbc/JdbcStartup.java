package io.mycat.test.jdbc;

import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jamie12221
 * @date 2019-05-19 18:23
 **/
public class JdbcStartup extends JdbcDao {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";

  @Test
  public void startUp() throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, new MycatMonitorCallback() {
      @Override
      public void onUnBindMySQLSession(MycatSession mycat, MySQLClientSession session) {

      }
    }, (session, sender, success, result, future) -> {
      try (Connection connection = getConnection()) {
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        connection.setAutoCommit(false);
        connection.createStatement().execute("select 1");
        connection.commit();
        compelete(future);
      } catch (Exception e) {
        Assert.fail(e.getMessage());
      }
    });
  }

}
