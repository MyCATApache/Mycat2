/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.test.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import io.mycat.MycatCore;
import io.mycat.MycatProxyBeanProviders;
import io.mycat.ProxyBeanProviders;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.monitor.AbstractMonitorCallback;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.Session;
import io.mycat.test.ModualTest;
import io.mycat.test.TestConnetionCallback;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jamie12221 date 2019-05-19 18:23
 **/
public class JdbcDao extends ModualTest {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";
  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcDao.class);


  @Test
  public void startRequestAndReponseWithSplitingPacketWithMultiSatement()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future, connection) -> {
          try (Statement statement = connection.createStatement()) {
            int splitPayloadSize = MySQLPacketSplitter.MAX_PACKET_SIZE - 1;
            int columnLength = 0xffffff;
            StringBuilder columnName = new StringBuilder(columnLength);
            StringBuilder largeSQLBuilder = new StringBuilder();

            //生成multi packet
            largeSQLBuilder.append("select ");
            int count = 1;
            while (true) {
              if (largeSQLBuilder.length() == splitPayloadSize) {
                count++;
              }
              if (columnName.length() == columnLength) {
                break;
              }
              columnName.append(count);
            }
            String sql = largeSQLBuilder.append(columnName).toString();
            String value = columnName.toString();
            ResultSet resultSet = statement.executeQuery(sql + ";" + sql);

            ResultSetMetaData metaData = resultSet.getMetaData();
            Assert.assertEquals(metaData.getColumnName(1), value);
            Assert.assertTrue(resultSet.next());
            String string = resultSet.getString(1);

            Assert.assertTrue(statement.getMoreResults());

            resultSet = statement.getResultSet();
            Assert.assertTrue(resultSet.next());
            metaData = resultSet.getMetaData();
            Assert.assertEquals(metaData.getColumnName(1), value);
            string = resultSet.getString(1);
          } finally {
            compelete(future);
          }
        }
    );
  }

  @Test
  public void startRequestAndReponseWithSplitingPacket()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future, connection) -> {
          try (Statement statement = connection.createStatement()) {
            int splitPayloadSize = MySQLPacketSplitter.MAX_PACKET_SIZE - 1;
            int columnLength = 1000;
            StringBuilder columnName = new StringBuilder(columnLength);
            StringBuilder largeSQLBuilder = new StringBuilder();
            largeSQLBuilder.append("select ");
            int count = 1;
            while (true) {
              if (largeSQLBuilder.length() == splitPayloadSize) {
                count++;
              }
              if (columnName.length() == columnLength) {
                break;
              }
              columnName.append(count);
            }
            String sql = largeSQLBuilder.append(columnName).toString();
            String value = columnName.toString();
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            Assert.assertEquals(metaData.getColumnName(1), value);
            Assert.assertTrue(resultSet.next());
            String string = resultSet.getString(1);
          } finally {
            compelete(future);
          }
        });
  }

  /**
   * CREATE TABLE `travelrecord` ( `id` bigint(20) NOT NULL, `user_id` varchar(100) DEFAULT NULL,
   * `traveldate` date DEFAULT NULL, `fee` decimal(10,0) DEFAULT NULL, `days` int(11) DEFAULT NULL,
   * `blob` longblob DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
   */
  @Test
  public void bigResultSet()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future, connection) -> {
          try (Statement statement = connection.createStatement()) {
            statement.execute("truncate travelrecord;");
            byte[] bytes = new byte[0xffffff];
            Arrays.fill(bytes, (byte) 0xff);
            String blob = new String(bytes);
            for (int i = 0; i < 2; i++) {
              String s1 =
                  "INSERT INTO `travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`, `blob`) "
                      + "VALUES ('"
                      + i
                      + "', '"
                      + "1"
                      + "', '"
                      + "2019-05-07"
                      + "', '"
                      + "1"
                      + "', '"
                      + "1"
                      + "', '"
                      + blob
                      + "');";
              statement.execute(s1);
            }
            ResultSet resultSet = statement.executeQuery(
                "select * from travelrecord;");
          } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
          } finally {
            compelete(future);
          }
        }
    );
  }

  @Test
  public void onAuthReadExpection()
      throws IOException, ExecutionException, InterruptedException {
    Runnable expectClear = mock(Runnable.class);
    Runnable expectClose = mock(Runnable.class);
    AbstractMonitorCallback callback = spy(new AbstractMonitorCallback() {
      @Override
      public void onFrontRead(Session session, ByteBuffer view, int startIndex, int len) {
        throw new RuntimeException("test exception");
      }

      @Override
      public void onAuthHandlerClear(Session session) {
        expectClear.run();
      }

      @Override
      public void onCloseMycatSession(MycatSession mycat, boolean normal, String reason) {
        expectClose.run();
      }
    });
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), callback,
        (future) -> {
          try (Connection connection = getConnection()) {

          } catch (Exception e) {
            e.printStackTrace();
            verify(expectClear).run();
            verify(expectClose).run();
          } finally {
            compelete(future);
          }

        }
    );
  }

  final static String url = "jdbc:mysql://localhost:8066/TESTDB?useServerPrepStmts=true&useCursorFetch=true&serverTimezone=UTC&allowMultiQueries=false&useBatchMultiSend=false&characterEncoding=utf8";
  final static String username = "root";
  final static String password = "123456";

  static {
    // 加载可能的驱动
    List<String> drivers = Arrays.asList(
        "com.mysql.jdbc.Driver");

    for (String driver : drivers) {
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException ignored) {
      }
    }
  }

  public static void loadModule(String module, ProxyBeanProviders proxyBeanProviders,
      MycatMonitorCallback callback,
      TestConnetionCallback task) throws InterruptedException, ExecutionException, IOException {
    ModualTest.loadModule(module, proxyBeanProviders, callback, new TestGettingConnetionCallback() {
      @Override
      public void test(Object future) {
        try (Connection connection = getConnection()) {
          task.test(future, connection);
        } catch (Exception e) {
          e.printStackTrace();
          Assert.fail(e.toString());
        } finally {
          MycatCore.exit();
          compelete(future);
        }
      }
    });
  }

//  public static void main(String[] args) {
//    try (Connection connection = getAutocommitConnection()) {
//      connection.setAutoCommit(false);
//      Statement statement = connection.createStatement();
//      ResultSet resultSet = statement.executeQuery("SELECT `id`, `topid` FROM `test` FOR UPDATE;");
//      connection.commit();
//    } catch (Exception e) {
//      e.printStackTrace();
//      Assert.fail(e.toString());
//    } finally {
//      MycatCore.exit();
//
//    }
//  }

  public static String getUrl() {
    return url;
  }

  public static String getUsername() {
    return username;
  }

  public static String getPassword() {
    return password;
  }

  public static Connection getConnection() throws SQLException {
    Connection connection = null;
    Properties properties = new Properties();
    properties.put("user", getUsername());
    properties.put("password", getPassword());
    properties.put("useBatchMultiSend", "false");
    properties.put("usePipelineAuth", "false");
    connection = DriverManager
        .getConnection(getUrl(), properties);
    return connection;
  }


  @Test
  public void perTest() throws InterruptedException, ExecutionException, IOException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future) -> {
//          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
          int count = 5000;
          CountDownLatch latch=new CountDownLatch(count);
          for (int i = 0; i < count; i++) {
            int index = i;
            new Thread(() -> {
              try (Connection connection = getConnection()) {
                for (int j = 0; j < 1; j++) {
                  connection.setAutoCommit(false);
                  try (Statement statement = connection.createStatement()) {
                    statement.execute("SELECT 1");//"SELECT * FROM `TESTDB1`.`travelrecord` LIMIT 0, 100000"
                  }
                  connection.commit();
                  LOGGER.info("{}", j);
                }
                latch.countDown();
                LOGGER.info("connectId:{} end", index);
              } catch (Exception e) {
                LOGGER.error("{}", e);
                return;
              }
            }).start();
          //  Thread.sleep(1000);
          }
          latch.await();
          LOGGER.info("success!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    );

  }

  @Test
  public void onMycatHandlerReadExpection()
      throws IOException, ExecutionException, InterruptedException {
    Runnable expectClear = mock(Runnable.class);
    Runnable expectClose = mock(Runnable.class);
    AbstractMonitorCallback callback = spy(new AbstractMonitorCallback() {
      @Override
      public void onCommandStart(MycatSession mycat) {
        throw new RuntimeException("test exception");
      }

      @Override
      public void onMycatHandlerClear(Session session) {
        expectClear.run();
      }

      @Override
      public void onCloseMycatSession(MycatSession mycat, boolean normal, String reason) {
        expectClose.run();
      }
    });
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), callback,
        (future) -> {
          try (Connection connection = getConnection()) {
            connection.createStatement().execute("select 1");
          } catch (Exception e) {
            e.printStackTrace();
            verify(expectClear).run();
            verify(expectClose).run();
          } finally {
            compelete(future);
          }

        }
    );
  }

  @Test
  public void onGetBackendExpection()
      throws IOException, ExecutionException, InterruptedException {
    Runnable expectMysqlClose = mock(Runnable.class);
    AbstractMonitorCallback callback = spy(new AbstractMonitorCallback() {

      @Override
      public void onBindMySQLSession(MycatSession mycat, MySQLClientSession session) {
        throw new RuntimeException("test exception");
      }

      @Override
      public void onCloseMysqlSession(MySQLClientSession session, boolean noraml, String reson) {
        expectMysqlClose.run();
      }
    });
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), callback,
        (future) -> {
          try (Connection connection = getConnection()) {
            connection.createStatement().execute("select 1");
          } catch (Exception e) {
            e.printStackTrace();
            verify(expectMysqlClose).run();
          } finally {
            compelete(future);
          }
        }
    );
  }


  @Test
  public void onProxyBackendReadExpection()
      throws IOException, ExecutionException, InterruptedException {
    AtomicInteger expectClear = new AtomicInteger(0);
    AtomicInteger expectClose = new AtomicInteger(0);
    AbstractMonitorCallback callback = new AbstractMonitorCallback() {
      Session session;

      @Override
      public void onPacketExchangerRead(Session session) {
        Assert.assertNull(this.session);
        this.session = session;
        throw new RuntimeException("test exception");
      }

      @Override
      public void onPacketExchangerClear(Session session) {
        if (expectClear.get() == 0) {
          Assert.assertSame(this.session, session);
          expectClear.incrementAndGet();
        }
      }

      @Override
      public void onCloseMysqlSession(MySQLClientSession session, boolean noraml, String reson) {
        Assert.assertSame(this.session, session);
        expectClose.incrementAndGet();
      }

      @Override
      public void onCloseMycatSession(MycatSession mycat, boolean normal, String reason) {
        super.onCloseMycatSession(mycat, normal, reason);
      }
    };
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), callback,
        (future) -> {
          try (Connection connection = getConnection()) {
            connection.createStatement().execute("select 1");
          } catch (Exception e) {
            Assert.assertTrue(expectClear.get() > 0);
            Assert.assertTrue(expectClose.get() > 0);
          } finally {
            compelete(future);
          }
        }
    );
  }

  @Test
  public void prepareStatement()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future, connection) -> {
          String sql =
              "INSERT INTO `travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`, `blob`) "
                  + "VALUES ("
                  + "?"
                  + ", '"
                  + "1"
                  + "', '"
                  + "2019-05-07"
                  + "', '"
                  + "1"
                  + "', '"
                  + "1"
                  + "', "
                  + "?"
                  + ");";
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 1);
            ByteOutputStream out = new ByteOutputStream();
            out.write(IntStream.range(0, 18193).mapToObj(i -> String.valueOf(i)).collect(
                Collectors.joining()).getBytes());
            ByteInputStream inputStream = out.newInputStream();
            statement.setBlob(2, inputStream);
            statement.execute();
            System.out.println("---------------");
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            compelete(future);
          }
        });
  }


  @Test
  public void cursor()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future, connection) -> {
          try (Statement statement = connection.createStatement()) {
            statement.execute("truncate travelrecord;");
            for (int i = 0; i < 10; i++) {
              String s1 =
                  "INSERT INTO `travelrecord` (`id`, `user_id`, `traveldate`, `fee`, `days`, `blob`) "
                      + "VALUES ('"
                      + i
                      + "', '"
                      + "1999999999999999"
                      + "', '"
                      + "2019-05-07"
                      + "', '"
                      + "1"
                      + "', '"
                      + "1"
                      + "', '"
                      + '1'
                      + "');";
              statement.execute(s1);
            }
            String sql = "select * from travelrecord;";

            PreparedStatement pstat = connection
                .prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            //最大查询到第几条记录
            pstat.setMaxRows(4);
            pstat.setFetchSize(1);

            ResultSet resultSet = pstat.executeQuery();

            while (resultSet.next()) {
              System.out.println("------------------");
              System.out.println(resultSet.getString(1));
              System.out.println("------------------");
            }
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            compelete(future);
          }
        });
  }

//  @Test
//  public void loadata()
//      throws IOException, ExecutionException, InterruptedException {
//    loadModule(DB_IN_ONE_SERVER, MycatProxyBeanProviders.INSTANCE, new MycatMonitorLogCallback(),
//        (future, connection) -> {
//          String loadDataSql = "LOAD DATA LOCAL INFILE 'd:/sql.csv' IGNORE INTO TABLE travelrecord (id,user_id,traveldate,fee,days,`blob`)";
//          try (PreparedStatement statement = connection.PREPARE_STATEMENT(loadDataSql)) {
//            ClientPreparedStatement preparedStatement = (ClientPreparedStatement) statement;
//            ByteInputStream inputStream = new ByteInputStream();
//            inputStream.setBuf("3,121,2011-02-03,123,2,1".getBytes());
//            preparedStatement.setLocalInfileInputStream(inputStream);
//            preparedStatement.EXECUTE();
//          }catch (Exception e){
//            e.printStackTrace();
//          }finally {
//            compelete(future);
//          }
//        });
//  }

  @Test
  public void jtaTest() throws InterruptedException, ExecutionException, IOException {
    AtomicInteger atomicInteger = new AtomicInteger(0);
    int count = 1;
    CountDownLatch latch = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      int index = i;
      new Thread(() -> {
        for (int j = 0; j < 1000; j++) {
          try (Connection connection = getConnection()) {
//            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
              statement.execute("SELECT 1");//SELECT * FROM `TESTDB1`.`travelrecord` LIMIT 0, 100000
//              statement.execute(" INSERT INTO `travelrecord` (`id`) VALUES ('2'); ");
//              statement.execute(" INSERT INTO `travelrecord2` (`id`) VALUES ('3'); ");
            }
//            connection.commit();
            LOGGER.info("connectId:{} per", index);
            atomicInteger.incrementAndGet();
          } catch (Exception e) {
            LOGGER.error("{}", e);
            return;
          }
        }
        LOGGER.info("connectId:{} end", index);
        latch.countDown();
      }).start();
    //  Thread.sleep(100);
    }
    latch.await();
    LOGGER.info("success!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
  }
}
