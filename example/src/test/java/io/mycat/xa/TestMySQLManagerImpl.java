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
package io.mycat.xa;

import cn.mycat.vertx.xa.SimpleConfig;
import io.mycat.MetaClusterCurrent;
import io.mycat.commands.AbstractMySQLManagerImpl;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.InstanceType;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mysqlclient.MySQLAuthenticationPlugin;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TestMySQLManagerImpl extends AbstractMySQLManagerImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestMySQLManagerImpl.class);
    private final ConcurrentHashMap<String, MySQLPool> nameMap = new ConcurrentHashMap<>();

    public TestMySQLManagerImpl(List<SimpleConfig> configList) {
        Objects.requireNonNull(configList);
        for (SimpleConfig simpleConfig : configList) {
            String name = simpleConfig.getName();
            MySQLPool pool = getMySQLPool(simpleConfig.getPort(), simpleConfig.getHost(), simpleConfig.getDatabase(), simpleConfig.getUser(), simpleConfig.getPassword(), simpleConfig.getMaxSize());
            nameMap.put(name, pool);
        }
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
        return MySQLPool.pool(connectOptions, poolOptions);
    }


    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        return nameMap.get(targetName).getConnection();
    }

    @Override
    public int getSessionCount(String targetName) {
        return 0;
    }

    @Override
    @SneakyThrows
    public Map<String, java.sql.Connection> getWriteableConnectionMap() {
        ConcurrentHashMap.KeySetView<String, MySQLPool> strings = nameMap.keySet();
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        Map<String, JdbcDataSource> datasourceInfo = jdbcConnectionManager.getDatasourceInfo();
        HashMap<String, Connection> map = new HashMap<>();
        for (String string : strings) {
            JdbcDataSource jdbcDataSource = datasourceInfo.get(string);
            DatasourceConfig config = jdbcDataSource.getConfig();
            if (jdbcDataSource.isMySQLType()) {
                if (InstanceType.valueOf(Optional.ofNullable(config.getInstanceType()).orElse("READ_WRITE").toUpperCase()).isWriteType()) {
                    Connection connection = jdbcDataSource.getDataSource().getConnection();
                    map.put(string, connection);
                }
            }
        }
        return map;
    }

    @Override
    public Future<Void> close() {
        return null;
    }

    @Override
    public Future<Map<String, Integer>> computeConnectionUsageSnapshot() {
        return null;
    }
}
