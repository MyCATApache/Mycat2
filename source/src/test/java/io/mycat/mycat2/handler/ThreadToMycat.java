package io.mycat.mycat2.handler;

/**
 * <dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
   <!--  <version>5.1.34</version> -->
   <!-- <version>5.1.39</version> -->
   <version>6.0.6</version>
    <!-- <version>8.0.13</version> -->
 </dependency>
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

public class ThreadToMycat extends Thread {
  CountDownLatch count;

  public ThreadToMycat(CountDownLatch count) {
    this.count = count;
  }

  public void run() {
    try {
      System.out.println(count.getCount());
      count.await();
    } catch (InterruptedException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }
    String url ="jdbc:mysql://127.0.0.1:8067/myfly?useSSL=false&useUnicode=true&characterEncoding=utf8";
//    String url ="jdbc:mysql://192.168.8.116/db2";
//    String url = "jdbc:mysql://127.0.0.1:8066/mycatdb?useUnicode=true&characterEncoding=utf8";
     String name = "com.mysql.cj.jdbc.Driver";
//    String name = "com.mysql.jdbc.Driver";
    String user = "root";
    String password = "123456";
    Connection conn = null;
    try {
      Class.forName(name);
      conn = DriverManager.getConnection(url, user, password);// 获取连接
      conn.setAutoCommit(false);// 关闭自动提交，不然conn.commit()运行到这句会报错
    } catch (ClassNotFoundException e1) {
      e1.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    if (conn != null) {
      Long startTime = System.currentTimeMillis();// 开始时间
      String sql = "select id from message";// SQL语句
      String id = null;
      try {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);// 获取结果集
//        if (rs.next()) {
//          id = rs.getString("id");
//        }
        conn.commit();
//        stmt.close();
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
        try {
          conn.rollback();
        } catch (SQLException e1) {
          e1.printStackTrace();
        }
      }
      Long end = System.currentTimeMillis();
      System.out.println(currentThread().getName() + "  查询结果:" + id + "   开始时间:" + startTime
          + "  结束时间:" + end + "  用时:" + (end - startTime) + "ms");


    } else {
      System.out.println(currentThread().getName() + "mycat连接失败:");
    }
  }

  public static void main(String[] args) {
    int num = 10;
    CountDownLatch count = new CountDownLatch(num);
    for (int i = 1; i <= num; i++) {
      new ThreadToMycat(count).start();
      count.countDown();
    }
  }
}
