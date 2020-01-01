//package io.mycat;
//
//import io.mycat.bindThread.BindThreadKey;
//import io.mycat.datasource.jdbc.JdbcRuntime;
//import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
//import io.mycat.datasource.jdbc.datasource.TransactionSession;
//import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
//import io.mycat.datasource.jdbc.thread.GProcess;
//import io.mycat.replica.ReplicaSelectorRuntime;
//
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ThreadLocalRandom;
//
//public class GRuntimeTest {
//
//  public static void main(String[] args) throws InterruptedException {
//    ReplicaSelectorRuntime.INSTCANE.load();
//    JdbcRuntime.INSTACNE.load(ConfigRuntime.INSTCANE.load());
//    JdbcDataSource ds1 = JdbcRuntime.INSTACNE.getJdbcDatasourceByDataNodeName("dn1", null);
//    JdbcDataSource ds2 = JdbcRuntime.INSTACNE.getJdbcDatasourceByDataNodeName("dn2", null);
//    CountDownLatch countDownLatch = new CountDownLatch(1000);
//    for (int i = 0; i < 1000; i++) {
//      BindThreadKey id = id();
//      JdbcRuntime.INSTACNE.run(id, new GProcess() {
//
//        @Override
//        public void accept(BindThreadKey key, TransactionSession session) {
//          session.begin();
//          DsConnection c1 = session.getConnection(ds1);
//          JdbcRowBaseIteratorImpl jdbcRowBaseIterator = c1.executeQuery("select 1");
//          List<Map<String, Object>> resultSetMap = jdbcRowBaseIterator.getResultSetMap();
//          DsConnection c2 = session.getConnection(ds2);
//          List<Map<String, Object>> resultSetMap1 = c2.executeQuery("select 1").getResultSetMap();
//          session.commit();
//          System.out.println(resultSetMap1);
//          countDownLatch.countDown();
//          System.out.println("-----------------" + countDownLatch);
//        }
//        @Override
//        public void onException(BindThreadKey key, Exception e) {
//          System.out.println(e);
//        }
//      });
//    }
//    countDownLatch.await();
//    System.out.println("----------------------end-------------------------");
//  }
//
//  private static BindThreadKey id() {
//    return new BindThreadKey() {
//      final int id = ThreadLocalRandom.current().nextInt();
//
//      @Override
//      public int hashCode() {
//        return id;
//      }
//
//      @Override
//      public boolean equals(Object obj) {
//        return this == obj;
//      }
//
//      @Override
//      public boolean checkOkInBind() {
//        return true;
//      }
//    };
//  }
//}