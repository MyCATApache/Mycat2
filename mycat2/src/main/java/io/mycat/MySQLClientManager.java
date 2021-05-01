/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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
