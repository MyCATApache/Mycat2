package io.mycat.client;

import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

public class ClientTest {
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
}
