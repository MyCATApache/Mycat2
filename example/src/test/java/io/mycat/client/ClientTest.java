package io.mycat.client;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.zaxxer.hikari.HikariDataSource;
import io.mycat.assemble.MycatTest;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClientTest implements MycatTest {
    @Test
    public void testDruid() throws Exception {
        try(DruidDataSource ds = new DruidDataSource()){
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
        client.close().toCompletionStage().toCompletableFuture().get(1,TimeUnit.SECONDS);
    }
    @Test
    public void tesHikariCP() throws Exception {
        try(HikariDataSource ds = new HikariDataSource()){
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
