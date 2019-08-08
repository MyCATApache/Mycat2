package io.mycat;

import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.connection.AbsractConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.manager.TransactionProcessJob;
import io.mycat.datasource.jdbc.manager.TransactionProcessKey;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import io.mycat.datasource.jdbc.session.TransactionSession;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

public class GRuntimeTest {

  public static void main(String[] args) throws InterruptedException {

    JdbcDataSource ds1 = GRuntime.INSTACNE.getJdbcDatasourceByDataNodeName("dn1", null);
    JdbcDataSource ds2 = GRuntime.INSTACNE.getJdbcDatasourceByDataNodeName("dn2", null);
    CountDownLatch countDownLatch = new CountDownLatch(100000);
    for (int i = 0; i < 100000; i++) {
      TransactionProcessKey id = id();
      GRuntime.INSTACNE.run(id, new TransactionProcessJob() {
        @Override
        public void accept(TransactionProcessKey key, TransactionSession session) {
          session.begin();
          AbsractConnection c1 = session.getConnection(ds1);
          JdbcRowBaseIteratorImpl jdbcRowBaseIterator = c1.executeQuery("select 1");
          List<Map<String, Object>> resultSetMap = jdbcRowBaseIterator.getResultSetMap();
          AbsractConnection c2 = session.getConnection(ds2);
          List<Map<String, Object>> resultSetMap1 = c2.executeQuery("select 1").getResultSetMap();
          session.commit();
          System.out.println(resultSetMap1);
          countDownLatch.countDown();
        }

        @Override
        public void onException(TransactionProcessKey key, Exception e) {
          System.out.println(e);
        }
      });
    }
    countDownLatch.await();
    System.out.println("----------------------end-------------------------");
  }

  private static TransactionProcessKey id() {
    return new TransactionProcessKey() {
      final int id = ThreadLocalRandom.current().nextInt();

      @Override
      public int hashCode() {
        return id;
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }
    };
  }
}