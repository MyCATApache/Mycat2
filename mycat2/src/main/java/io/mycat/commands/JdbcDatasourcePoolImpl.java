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

import com.alibaba.druid.util.JdbcUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JdbcDatasourcePoolImpl extends AbstractMycatDatasourcePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcDatasourcePoolImpl.class);
    List<Object> list = Collections.synchronizedList(new ArrayList<>());
    public JdbcDatasourcePoolImpl(String targetName) {
        super(targetName);
    }

    @Override
    public Future<NewMycatConnection> getConnection() {
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("JdbcDatasourcePoolImpl {}  : size: {}",getTargetName(),list.size());
            LOGGER.debug("JdbcDatasourcePoolImpl {}  : {}",getTargetName(),list);
        }
        try {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            DefaultConnection defaultConnection = jdbcConnectionManager.getConnection(targetName);
            DatabaseInstanceEntry stat = DatabaseInstanceEntry.stat(targetName);
            stat.plusCon();
            stat.plusQps();
            NewMycatConnectionImpl newMycatConnection = new NewMycatConnectionImpl(targetName, defaultConnection.getRawConnection()) {
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
                    return super.getFuture().transform(result -> {
                        stat.decCon();
                        JdbcUtils.close(getResultSet());
                        defaultConnection.close();

                        if (LOGGER.isDebugEnabled()) {
                            list.remove(this);
                            LOGGER.debug("JdbcDatasourcePoolImpl {}  : size: {}",getTargetName(),list.size());
                            LOGGER.debug("JdbcDatasourcePoolImpl {}  : {}",getTargetName(),list);
                        }

                        return Future.succeededFuture();
                    });
                }

                @Override
                public void abandonConnection() {
                    try{
                        defaultConnection.close();

                        if (LOGGER.isDebugEnabled()) {
                            list.remove(this);
                            LOGGER.debug("JdbcDatasourcePoolImpl {}  : size: {}",getTargetName(),list.size());
                            LOGGER.debug("JdbcDatasourcePoolImpl {}  : {}",getTargetName(),list);
                        }
                    }finally {
                        stat.decCon();
                    }
                }
            };
            list.add(newMycatConnection);
            return Future.succeededFuture(new ThreadMycatConnectionImplWrapper(stat, newMycatConnection));
        } catch (Throwable throwable) {
            return Future.failedFuture(throwable);
        }
    }

    @Override
    public int getAvailableNumber() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(targetName);
        return jdbcDataSource.getMaxCon() - jdbcDataSource.getUsedCount();
    }

    @Override
    public int getUsedNumber() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        JdbcDataSource jdbcDataSource = jdbcConnectionManager.getDatasourceInfo().get(targetName);
        return jdbcDataSource.getUsedCount();
    }

    @Override
    public void close() {

    }
}
