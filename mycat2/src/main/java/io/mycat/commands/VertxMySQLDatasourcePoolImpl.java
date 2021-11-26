/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.commands;

import com.mysql.cj.conf.ConnectionUrlParser;
import com.mysql.cj.conf.HostInfo;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.DatasourceConfig;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.monitor.DatabaseInstanceEntry;
import io.mycat.monitor.InstanceMonitor;
import io.mycat.monitor.ThreadMycatConnectionImplWrapper;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.NewMycatConnectionImpl;
import io.mycat.newquery.NewVertxConnectionImpl;
import io.mycat.util.JsonUtil;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.MySQLAuthenticationPlugin;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.mysqlclient.SslMode;
import io.vertx.mysqlclient.impl.MySQLConnectionImpl;
import io.vertx.mysqlclient.impl.MySQLPoolImpl;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnection;

import java.util.Map;
import java.util.function.Function;

public class VertxMySQLDatasourcePoolImpl extends AbstractMycatDatasourcePool {
    final MySQLPoolImpl mySQLPool;
    final DatasourceConfig config;

    public VertxMySQLDatasourcePoolImpl(DatasourceConfig config, String targetName) {
        super(targetName);
        this.config = config;
        ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(config.getUrl());
        HostInfo hostInfo = connectionUrlParser.getHosts().get(0);
        MySQLConnectOptions connectOptions = new MySQLConnectOptions()
                .setPort(hostInfo.getPort())
                .setHost(hostInfo.getHost())
                .setDatabase(hostInfo.getDatabase())
                .setUser(config.getUser())
                .setPassword(config.getPassword())
//                .setCollation("utf8mb4")
                .setCharset("utf8")
                .setUseAffectedRows(true);
        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(config.getMaxCon())
                .setIdleTimeout((int) config.getIdleTimeout());

        this.mySQLPool = (MySQLPoolImpl)MySQLPool.pool(connectOptions, poolOptions);

    }

    @Override
    public Future<NewMycatConnection> getConnection() {
       return mySQLPool.getConnection().map(sqlConnection -> new NewVertxConnectionImpl((MySQLConnectionImpl)sqlConnection));
    }

    @Override
    public Integer getAvailableNumber() {
        return mySQLPool.size();
    }

    @Override
    public Integer getUsedNumber() {
        return mySQLPool.size();
    }

    @Override
    public void close() {
        this.mySQLPool.close();
    }
}
