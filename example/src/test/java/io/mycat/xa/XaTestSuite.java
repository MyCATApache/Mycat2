/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mycat.xa;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.SimpleConfig;
import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import cn.mycat.vertx.xa.impl.XaLogImpl;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.util.JsonUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

@ExtendWith(VertxExtension.class)
public abstract class XaTestSuite {
    private final MySQLManager mySQLManager;
    private final XaLog xaLog;
    private final BiFunction<MySQLManager, XaLog, XaSqlConnection> factory;
    public static  final String DB1 = System.getProperty("db1", "jdbc:mysql://127.0.0.1:3306/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");
    public static  final  String DB2 = System.getProperty("db2", "jdbc:mysql://127.0.0.1:3307/mysql?username=root&password=123456&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true");


    public XaTestSuite(MySQLManager mySQLManager,
                       BiFunction<MySQLManager,XaLog,XaSqlConnection> factory) throws Exception {
        this.mySQLManager = mySQLManager;
        this.xaLog = XaLogImpl.createXaLog(mySQLManager);
        this.factory = factory;

        Connection mySQLConnection = getMySQLConnection(DB2);
        extracteInitSql(mySQLConnection);
        mySQLConnection.close();
        mySQLConnection = getMySQLConnection(DB1);
        extracteInitSql(mySQLConnection);
        mySQLConnection.close();
    }

    private void extracteInitSql(Connection mySQLConnection) throws SQLException {
        JdbcUtils.execute(mySQLConnection, "CREATE DATABASE IF NOT EXISTS db1;");
        JdbcUtils.execute(mySQLConnection, "CREATE TABLE IF NOT EXISTS db1.`travelrecord` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8");
    }


    @Test
    public void begin(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertEquals(baseXaSqlConnection.isInTransaction(), true);
                baseXaSqlConnection.close();
                testContext.completeNow();
            }
        });
    }

    @Test
    public void beginCommit(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> baseXaSqlConnection.commit(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertEquals(baseXaSqlConnection.isInTransaction(), false);
                baseXaSqlConnection.close();
                testContext.completeNow();
            }
        }));
    }

    @Test
    public void beginRollback(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> baseXaSqlConnection.rollback(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertEquals(baseXaSqlConnection.isInTransaction(), false);
                baseXaSqlConnection.close();
                testContext.completeNow();
            }
        }));
    }

    @Test
    public void beginBegin(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                baseXaSqlConnection.begin(event1 -> {
                    Assertions.assertTrue(event1.failed());
                    testContext.completeNow();
                });

            }
        });
    }

    @Test
    public void rollback(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.rollback(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertTrue(event.succeeded());
                Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void commit(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.commit(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertTrue(event.succeeded());
                Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void close(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.close(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                Assertions.assertTrue(event.succeeded());
                Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                testContext.completeNow();
            }
        });
    }

    @Test
    public void closeInTranscation(VertxTestContext testContext) {
        XaSqlConnection baseXaSqlConnection = factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                baseXaSqlConnection.close(new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> event) {
                        Assertions.assertTrue(event.succeeded());
                        Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                        testContext.completeNow();
                    }
                });
            }
        });
    }

    @Test
    public void beginSingleTargetInsertCommit(VertxTestContext testContext) throws Exception {
        clearData();
        XaSqlConnection baseXaSqlConnection =  factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }).compose(connection -> {
                return connection.query("select id from db1.travelrecord").execute()
                        .compose(rows -> {
                            Assertions.assertEquals(1, rows.size());
                            return Future.succeededFuture(connection);
                        });
            }).onComplete(event13 -> {
                Assertions.assertTrue(event13.succeeded());
                baseXaSqlConnection.commit(event12 -> {
                    Assertions.assertTrue(event12.succeeded());
                    Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                    Future<SqlConnection> connectionFuture =
                            baseXaSqlConnection.getConnection("ds1");
                    connectionFuture
                            .compose(sqlConnection ->
                                    sqlConnection.query("select id from db1.travelrecord").execute())
                            .onComplete(event1 -> {
                                Assertions.assertTrue(event1.succeeded());
                                Assertions.assertEquals(1, event1.result().size());

                                testContext.completeNow();
                            });
                });
            });
        });
    }

    @Test
    public void beginDoubleTargetInsertCommit(VertxTestContext testContext) throws Exception {
        clearData();
        XaSqlConnection baseXaSqlConnection =  factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            Future<SqlConnection> ds2 = baseXaSqlConnection.getConnection("ds2");

            CompositeFuture all = CompositeFuture.all(ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }), ds2.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (2);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }));
            all.onComplete(event13 -> {
                Assertions.assertTrue(event13.succeeded());
                baseXaSqlConnection.commit(event12 -> {
                    Assertions.assertTrue(event12.succeeded());
                    Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                    Future<SqlConnection> connectionFuture =
                            baseXaSqlConnection.getConnection("ds2");
                    connectionFuture
                            .compose(sqlConnection ->
                                    sqlConnection.query("select id from db1.travelrecord").execute())
                            .onComplete(event1 -> {
                                Assertions.assertTrue(event1.succeeded());
                                Assertions.assertEquals(1, event1.result().size());

                                testContext.completeNow();
                            });
                });
            });
        });
    }
    @Test
    public void beginDoubleTargetInsertButStatementFail(VertxTestContext testContext) throws Exception {
        clearData();
        XaSqlConnection baseXaSqlConnection =  factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            Future<SqlConnection> ds2 = baseXaSqlConnection.getConnection("ds2");

            CompositeFuture all = CompositeFuture.all(ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }), ds2.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO  (id)\n" +
                                "                       VALUES\n" +
                                "                       (2/0);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }));
            all.onComplete(event13 -> {
                Assertions.assertTrue(event13.failed());
                baseXaSqlConnection.rollback(new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> event) {
                        Assertions.assertTrue(event.succeeded());
                        Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                        Future<SqlConnection> connectionFuture =
                                baseXaSqlConnection.getConnection("ds1");
                        connectionFuture
                                .compose(sqlConnection ->
                                        sqlConnection.query("select id from db1.travelrecord").execute())
                                .onComplete(event1 -> {
                                    Assertions.assertTrue(event1.succeeded());
                                    Assertions.assertEquals(0, event1.result().size());

                                    testContext.completeNow();
                                });
                    }
                });
            });
        });
    }
    @Test
    public void beginDoubleTargetInsertButPrepareFail(VertxTestContext testContext) throws Exception {
        clearData();
        XaSqlConnection baseXaSqlConnection =  factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            Future<SqlConnection> ds2 = baseXaSqlConnection.getConnection("ds2");

            CompositeFuture all = CompositeFuture.all(ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }), ds2.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (2);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }));
            all.onComplete(event13 -> {
                Assertions.assertTrue(event13.succeeded());
                Future<Void> future = baseXaSqlConnection.commitXa((c) -> Future.failedFuture("prepare fail"));
                future.onComplete(new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> event) {
                        Assertions.assertTrue(event.failed());
                        baseXaSqlConnection.rollback(new Handler<AsyncResult<Void>>() {
                            @Override
                            public void handle(AsyncResult<Void> event) {
                                Assertions.assertTrue(event.succeeded());
                                Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                                Future<SqlConnection> connectionFuture =
                                        baseXaSqlConnection.getConnection("ds1");
                                connectionFuture
                                        .compose(sqlConnection ->
                                                sqlConnection.query("select id from db1.travelrecord").execute())
                                        .onComplete(event1 -> {
                                            Assertions.assertTrue(event1.succeeded());
                                            Assertions.assertEquals(0, event1.result().size());

                                            testContext.completeNow();
                                        });
                            }
                        });
                    }
                });

            });
        });
    }
    @Test
    public void beginDoubleTargetInsertButCommitFail(VertxTestContext testContext) throws Exception {
        clearData();
        XaSqlConnection baseXaSqlConnection =  factory.apply(mySQLManager,xaLog);
        baseXaSqlConnection.begin(event -> {
            Assertions.assertTrue(event.succeeded());
            Future<SqlConnection> ds1 = baseXaSqlConnection.getConnection("ds1");
            Future<SqlConnection> ds2 = baseXaSqlConnection.getConnection("ds2");

            CompositeFuture all = CompositeFuture.all(ds1.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (1);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }), ds2.compose(connection -> {
                Future<RowSet<Row>> future = connection.query(
                        "INSERT INTO db1.travelrecord (id)\n" +
                                "                       VALUES\n" +
                                "                       (2);").execute();
                return future.compose(rowSet -> {
                    Assertions.assertEquals(1, rowSet.rowCount());
                    return Future.succeededFuture(connection);
                });
            }));
            all.onComplete(event13 -> {
                Assertions.assertTrue(event13.succeeded());
                Future<Void> future = baseXaSqlConnection.commitXa((log) -> Future.failedFuture("commit fail"));
                future.onComplete( new Handler<AsyncResult<Void>>() {
                            @Override
                            public void handle(AsyncResult<Void> event) {
                                Assertions.assertTrue(event.failed());
                                baseXaSqlConnection.commit(new Handler<AsyncResult<Void>>() {
                                    @Override
                                    public void handle(AsyncResult<Void> event) {
                                        Assertions.assertTrue(event.succeeded());

                                        Assertions.assertFalse(baseXaSqlConnection.isInTransaction());
                                        Future<SqlConnection> connectionFuture =
                                                baseXaSqlConnection.getConnection("ds1");
                                        connectionFuture
                                                .compose(sqlConnection ->
                                                        sqlConnection.query("select id from db1.travelrecord").execute())
                                                .onComplete(event1 -> {
                                                    Assertions.assertTrue(event1.succeeded());
                                                    Assertions.assertEquals(1, event1.result().size());

                                                    testContext.completeNow();
                                                });
                                    }
                                });
                            }
                        });

            });
        });
    }
    private void clearData() throws SQLException {
        try(Connection mySQLConnection = getMySQLConnection(DB2)){
            clearDb(mySQLConnection);
        }
        try(Connection mySQLConnection = getMySQLConnection(DB1)){
            clearDb(mySQLConnection);
        }
    }

    /**
     * GRANT XA_RECOVER_ADMIN ON *.* TO 'username'@'%';
     * FLUSH PRIVILEGES;
     * @param mySQLConnection
     * @throws SQLException
     */
    private void clearDb(Connection mySQLConnection) throws SQLException {
        List<Map<String, Object>> mapList = JdbcUtils.executeQuery(mySQLConnection, "XA RECOVER;", Collections.emptyList());
        for (Map<String, Object> i : mapList) {
                    JdbcUtils.execute(mySQLConnection, "xa rollback '" + i.get("data")+"'");
        }
        JdbcUtils.execute(mySQLConnection,"delete from db1.travelrecord");
    }

    public static SimpleConfig demoConfig(String name, int port) {
        SimpleConfig simpleConfig = new SimpleConfig(name, "127.0.0.1", port, "root", "123456", "mysql", 5);
        return simpleConfig;
    }

    public static Map<String, DruidDataSource> dsMap = new ConcurrentHashMap<>();


   static Connection getMySQLConnection(String url) {
        try {
            return dsMap.computeIfAbsent(url, new Function<String, DruidDataSource>() {
                @Override
                public DruidDataSource apply(String url) {
                    Map<String, String> urlParameters = JsonUtil.urlSplit(url);
                    String username = urlParameters.getOrDefault("username", "root");
                    String password = urlParameters.getOrDefault("password", "123456");

                    DruidDataSource dataSource = new DruidDataSource();
                    dataSource.setUrl(url);
                    dataSource.setUsername(username);
                    dataSource.setPassword(password);
                    dataSource.setLoginTimeout(5);
                    dataSource.setCheckExecuteTime(true);
                    dataSource.setMaxWait(TimeUnit.SECONDS.toMillis(10));
                    return dataSource;
                }
            }).getConnection();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
