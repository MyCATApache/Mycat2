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
                "select * from travelrecord;select * from travelrecord;");
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

  final static String url = "jdbc:mysql://localhost:8066/test?useServerPrepStmts=true&useCursorFetch=true&serverTimezone=UTC&allowMultiQueries=false";
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
    connection = DriverManager
        .getConnection(getUrl(), getUsername(),
            getPassword());
    return connection;
  }

  @Test
  public void perTest() throws InterruptedException, ExecutionException, IOException {
    loadModule(DB_IN_ONE_SERVER, new MycatProxyBeanProviders(), new MycatMonitorLogCallback(),
        (future) -> {
//          Thread.sleep(TimeUnit.SECONDS.toMillis(5));
          int count = 1;
          AtomicInteger atomicInteger = new AtomicInteger(0);
          for (int i = 0; i < count; i++) {
            int index = i;
            new Thread(() -> {
              try (Connection connection = getConnection()) {
                for (int j = 0; j < 100000; j++) {
                  connection.setAutoCommit(false);
                  connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                  try (Statement statement = connection.createStatement()) {
                    statement.execute("select 1");
                  }
                  connection.commit();
                }
                atomicInteger.incrementAndGet();
                LOGGER.info("connectId:{} end", index);
              } catch (Exception e) {
                LOGGER.error("{}", e);
                return;
              }
            }).start();
          }
          while (atomicInteger.get() != count) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
          }
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
//          try (PreparedStatement statement = connection.prepareStatement(loadDataSql)) {
//            ClientPreparedStatement preparedStatement = (ClientPreparedStatement) statement;
//            ByteInputStream inputStream = new ByteInputStream();
//            inputStream.setBuf("3,121,2011-02-03,123,2,1".getBytes());
//            preparedStatement.setLocalInfileInputStream(inputStream);
//            preparedStatement.execute();
//          }catch (Exception e){
//            e.printStackTrace();
//          }finally {
//            compelete(future);
//          }
//        });
//  }
}
