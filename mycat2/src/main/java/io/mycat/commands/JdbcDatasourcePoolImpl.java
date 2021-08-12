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
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.vertxmycat.JdbcMySqlConnection;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

public class JdbcDatasourcePoolImpl extends AbstractMycatDatasourcePool {
    public JdbcDatasourcePoolImpl(String targetName) {
        super(targetName);
    }

    @Override
    public Future<SqlConnection> getConnection() {
        try {
        return Future.succeededFuture(new JdbcMySqlConnection(targetName));
        } catch (Throwable throwable){
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
