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

import io.mycat.MycatProxyBeanProviders;
import io.mycat.beans.mysql.packet.MySQLPacketSplitter;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.monitor.MycatMonitorLogCallback;
import io.mycat.test.ModualTest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221 date 2019-05-19 18:23
 **/
public class JdbcDao extends ModualTest {

  final static String DB_IN_ONE_SERVER = "DB_IN_ONE_SERVER";
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDao.class);

  @Test
  public void startRequestAndReponseWithSplitingPacketWithMultiSatement()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, MycatProxyBeanProviders.INSTANCE, new MycatMonitorLogCallback(),
        new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object future, Object attr) {
            // successful
            try (Connection connection = getConnection()) {
              Statement statement = connection.createStatement();
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

              compelete(future);
            } catch (Exception e) {
              e.printStackTrace();
              Assert.fail(e.toString());
              compelete(future);
            }
          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {
            compelete(attr);
            e.printStackTrace();
            Assert.fail(e.toString());
          }
        });
  }

  @Test
  public void startRequestAndReponseWithSplitingPacket()
      throws IOException, ExecutionException, InterruptedException {
    loadModule(DB_IN_ONE_SERVER, MycatProxyBeanProviders.INSTANCE, new MycatMonitorLogCallback(),
        new AsyncTaskCallBack() {
          @Override
          public void onFinished(Object sender, Object future, Object attr) {
            // successful
            try (Connection connection = getConnection()) {
              Statement statement = connection.createStatement();
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
              compelete(future);
            } catch (Exception e) {
              e.printStackTrace();
              Assert.fail(e.toString());
              compelete(future);
            }
          }

          @Override
          public void onException(Exception e, Object sender, Object attr) {
            e.printStackTrace();
            Assert.fail(e.toString());
          }
        });
  }

  private void perTest(int count, AsyncTaskCallBackCounter callBackCounter) {
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

}
