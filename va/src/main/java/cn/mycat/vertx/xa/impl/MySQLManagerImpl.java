/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.SimpleConfig;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mysqlclient.MySQLAuthenticationPlugin;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MySQLManagerImpl implements MySQLManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLManagerImpl.class);
    private final ConcurrentHashMap<String, MySQLPool> nameMap = new ConcurrentHashMap<>();

    public MySQLManagerImpl(List<SimpleConfig> configList) {
        load(configList, true);
    }

    private void load(List<SimpleConfig> configList, boolean add) {
        Objects.requireNonNull(configList);
        for (SimpleConfig simpleConfig : configList) {
            String name = simpleConfig.getName();
            MySQLPool pool = getMySQLPool(simpleConfig.getPort(), simpleConfig.getHost(), simpleConfig.getDatabase(), simpleConfig.getUser(), simpleConfig.getPassword(), simpleConfig.getMaxSize());
            if (nameMap.containsKey(name) && add) {
                continue;
            }
            nameMap.put(name, pool);
        }
    }

    public void reload(List<SimpleConfig> configList) {
        nameMap.clear();
        load(configList, true);
    }

    private MySQLPool getMySQLPool(int port, String host, String database, String user, String password, int maxSize) {
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(port)
                .setAuthenticationPlugin(MySQLAuthenticationPlugin.MYSQL_NATIVE_PASSWORD)
                .setHost(host)
                .setDatabase(database)
                .setUser(user)
                .setPassword(password);
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(maxSize);
        Vertx owner = Optional.ofNullable(Vertx.currentContext()).map(i -> i.owner()).orElse(null);
        if (owner == null) {
            return MySQLPool.pool(connectOptions, poolOptions);
        }
        return MySQLPool.pool(owner, connectOptions, poolOptions);
    }


    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        return nameMap.get(targetName).getConnection();
    }

    @Override
    public Future<Map<String, SqlConnection>> getConnectionMap() {
        Enumeration<String> keys = this.nameMap.keys();
        HashSet<String> objects = new HashSet<>();
        while (keys.hasMoreElements()){
            objects.add(keys.nextElement());
        }
        return getMapFuture(objects);
    }


    @Override
    public Future<Void> close() {
        return CompositeFuture.all(nameMap.values().stream().map(i -> i.close()).collect(Collectors.toList())).mapEmpty();
    }

    @Override
    public void setTimer(long delay, Runnable handler) {
        Vertx.currentContext().owner().setTimer(delay, event -> handler.run());
    }

}
