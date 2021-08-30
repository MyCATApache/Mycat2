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

import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.monitor.DatabaseInstanceEntry;
import io.mycat.monitor.InstanceMonitor;
import io.mycat.monitor.ThreadMycatConnectionImplWrapper;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.NewMycatConnectionImpl;
import io.vertx.core.Future;

public class JdbcDatasourcePoolImpl extends AbstractMycatDatasourcePool {
    public JdbcDatasourcePoolImpl(String targetName) {
        super(targetName);
    }

    @Override
    public Future<NewMycatConnection> getConnection() {
        try {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            DefaultConnection connection = jdbcConnectionManager.getConnection(targetName);
            DatabaseInstanceEntry stat = DatabaseInstanceEntry.stat(targetName);
            stat.plusCon();
            stat.plusQps();
            NewMycatConnectionImpl newMycatConnection = new NewMycatConnectionImpl(connection.getRawConnection()) {
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
                    connection.close();
                    return Future.succeededFuture();
                }
            };
            return Future.succeededFuture(new ThreadMycatConnectionImplWrapper(stat,newMycatConnection));
        } catch (Throwable throwable) {
            return Future.failedFuture(throwable);
        }
    }

    @Override
    public Integer getAvailableNumber() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(targetName);
        return jdbcDataSource.getMaxCon() - jdbcDataSource.getUsedCount();
    }

    @Override
    public Integer getUsedNumber() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(targetName);
        return jdbcDataSource.getUsedCount();
    }
}
