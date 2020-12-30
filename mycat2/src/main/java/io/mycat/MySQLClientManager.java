package io.mycat;

import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLAuthenticationPlugin;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MySQLClientManager {
    final ConcurrentHashMap<String, MySQLPool> map = new ConcurrentHashMap<>();

    public MySQLClientManager(Vertx vertx) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(3307)
                .setUser("root")
                .setPassword("123456")
                .setUseAffectedRows(true)
                .setAuthenticationPlugin(MySQLAuthenticationPlugin.MYSQL_NATIVE_PASSWORD);
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(5);
        MySQLPool mySQLPool = MySQLPool.pool(vertx,connectOptions, poolOptions);
        map.put("ds",mySQLPool);
    }
}
