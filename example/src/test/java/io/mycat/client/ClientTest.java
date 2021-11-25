package io.mycat.client;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.mycat.DrdsSqlWithParams;
import io.mycat.assemble.MycatTest;
import io.mycat.calcite.DrdsRunnerHelper;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClientTest implements MycatTest {
    @Test
    public void testDruid() throws Exception {
        try (DruidDataSource ds = new DruidDataSource()) {
            ds.setUrl(DB_MYCAT);
            ds.setUsername("root");
            ds.setPassword("123456");

            List<Map<String, Object>> maps = JdbcUtils.executeQuery(ds, "select 1");
            Connection connection = ds.getConnection();
            int transactionIsolation = connection.getTransactionIsolation();
            System.out.println();
        }
    }

    @Test
    public void testDBeaver() throws Exception {
        /**
         *

         SELECT *
         FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = 'db1'
         AND TABLE_NAME = 'www'
         ORDER BY ORDINAL_POSITION
         *
         *
         */
        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse("SELECT @@global.character_set_server, @@global.collation_server", null);
        Assert.assertEquals("select @@global.character_set_server, @@global.collation_server",drdsSqlWithParams.getParameterizedSql());
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT)) {
//            Statement statement = mySQLConnection.createStatement();
//            ResultSet resultSet = statement.executeQuery("SELECT * FROM `information_schema`.`CHARACTER_SETS` LIMIT 0, 1000; ");
//            List<String> res = new ArrayList<>();
//            while (resultSet.next()) {
//                int columnCount = resultSet.getMetaData().getColumnCount();
//                Object[] objects = new Object[columnCount];
//                for (int i = 0; i < columnCount; i++) {
//                    objects[i] = resultSet.getObject(i + 1);
//                }
//                String collect = Stream.of(objects).map(i -> {
//                    if (i instanceof String) {
//                        return "\"" + i + "\"";
//                    }
//                    return i.toString();
//                }).collect(Collectors.joining(","));
//                res.add("r.add(new Object[]{"+collect+"});");
//            }
//            System.out.println(String.join("",res));
//            List<Map<String, Object>> show_collation = JdbcUtils.executeQuery(mySQLConnection, "SHOW COLLATION", Collections.emptyList());

            execute(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Main */ SET autocommit=1");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SELECT DATABASE()");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SHOW ENGINES");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SHOW CHARSET");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SHOW COLLATION");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SELECT @@GLOBAL.character_set_server,@@GLOBAL.collation_server");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SHOW PLUGINS");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SHOW VARIABLES LIKE 'lower_case_table_names'");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ show databases");
            executeQuery(mySQLConnection, "/* ApplicationName=DBeaver 21.2.5 - Metadata */ SELECT * FROM information_schema.TABLES t\n" +
                    "WHERE\n" +
                    "\tt.TABLE_SCHEMA = 'information_schema'\n" +
                    "\tAND t.TABLE_NAME = 'CHECK_CONSTRAINTS'");
        }
    }

    @Test
    public void tesVertx() throws Exception {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(8066)
                .setHost("localhost")
                .setDatabase("mysql")
                .setUser("root")
                .setPassword("123456");

// Pool options
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(1);

// Create the client pool
        MySQLPool client = MySQLPool.pool(connectOptions, poolOptions);

// A simple query
        client
                .query("SELECT  1").execute().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        client.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.SECONDS);
    }

    @Test
    public void tesHikariCP() throws Exception {
        try (HikariDataSource ds = new HikariDataSource()) {
            ds.setJdbcUrl(DB_MYCAT);
            ds.setUsername("root");
            ds.setPassword("123456");

            List<Map<String, Object>> maps = JdbcUtils.executeQuery(ds, "select 1");
            Connection connection = ds.getConnection();
            int transactionIsolation = connection.getTransactionIsolation();
            System.out.println();
        }

    }
}
