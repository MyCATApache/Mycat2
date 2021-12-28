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
import io.mycat.monitor.DatabaseInstanceEntry;
import io.mycat.monitor.InstanceMonitor;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.NewVertxConnectionImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.mysqlclient.impl.MySQLConnectionImpl;
import io.vertx.mysqlclient.impl.MySQLPoolImpl;
import io.vertx.sqlclient.PoolOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class VertxMySQLDatasourcePoolImpl extends AbstractMycatDatasourcePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxMySQLDatasourcePoolImpl.class);

    final MySQLPoolImpl mySQLPool;
    final MySQLConnectOptions connectOptions;
    final PoolOptions poolOptions;

    public VertxMySQLDatasourcePoolImpl(DatasourceConfig config, String targetName) {
        super(targetName);

        ConnectionUrlParser connectionUrlParser = ConnectionUrlParser.parseConnectionString(config.getUrl());
        HostInfo hostInfo = connectionUrlParser.getHosts().get(0);
        connectOptions = new MySQLConnectOptions()
                .setPort(hostInfo.getPort())
                .setHost(hostInfo.getHost())
                .setDatabase(hostInfo.getDatabase())
                .setUser(config.getUser())
                .setPassword(config.getPassword())
                .setCachePreparedStatements(false)
                .setReconnectAttempts(config.getMaxRetryCount())
                .setConnectTimeout((int)config.getMaxConnectTimeout())
                .setTcpKeepAlive(true)
                .setSsl(false)
//                .setCollation("utf8mb4")
                .setCharset("utf8")
                .setUseAffectedRows(true);
        poolOptions = new PoolOptions()
//                .setEventLoopSize(8)
//                .setMaxSize(config.getMaxCon())
//                .setMaxWaitQueueSize(-1)
//                .se
//                .setIdleTimeout()
//                .setIdleTimeout((int) config.getIdleTimeout())
//                .setConnectionTimeout(config.getQueryTimeout())
//                .setMaxWaitQueueSize(65535)
        ;

        this.mySQLPool = getMySQLPool();
    }

    private MySQLPoolImpl getMySQLPool() {
        Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
        return (MySQLPoolImpl) MySQLPool.pool(connectOptions, poolOptions);
    }

    public VertxMySQLDatasourcePoolImpl(
            final MySQLConnectOptions connectOptions,
            final PoolOptions poolOptions,
            final String targetName) {
        super(targetName);
        this.connectOptions = connectOptions;
        this.poolOptions = poolOptions;
        this.mySQLPool = getMySQLPool();
    }

    @Override
    public Future<NewMycatConnection> getConnection() {
        LOGGER.debug("getConnection");
        Future<NewVertxConnectionImpl> map = mySQLPool.getConnection().map(sqlConnection -> {
            DatabaseInstanceEntry stat = DatabaseInstanceEntry.stat(targetName);
            stat.plusCon();
            stat.plusQps();
            return new NewVertxConnectionImpl((MySQLConnectionImpl) sqlConnection) {
                long start;

                @Override
                public void onSend() {
                    start = System.currentTimeMillis();
                }

                @Override
                public void onRev() {
                    long end = System.currentTimeMillis();
                    InstanceMonitor.plusPrt(end - start);
                }

                @Override
                public Future<Void> close() {

                    stat.decCon();
                    return super.close();
                }
            };
        });
        map = map.onFailure(new Handler<Throwable>() {
            @Override
            public void handle(Throwable event) {
                System.out.println(event);
            }
        });
        return (Future) map;
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
