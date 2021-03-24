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
package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.NativeMycatServer;
import io.mycat.proxy.MySQLDatasourcePool;
import io.mycat.vertxmycat.AbstractMySqlConnectionImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.SqlConnection;

public class NativeDatasourcePoolImpl extends MycatDatasourcePool {
    public NativeDatasourcePoolImpl(String targetName) {
        super(targetName);
    }

    @Override
    public Future<SqlConnection> getConnection() {
        return Future.future(promise -> {
            NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
            MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
            sqlDatasourcePool.createSession().flatMap(session -> {
                Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
                return vertx
                        .executeBlocking((Handler<Promise<SqlConnection>>) event -> event.complete(new AbstractMySqlConnectionImpl(session)));
            }).onComplete(promise);
        });
    }

    @Override
    public Integer getAvailableNumber() {
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
        return sqlDatasourcePool.getSessionLimitCount() - sqlDatasourcePool.currentSessionCount();
    }

    @Override
    public Integer getUsedNumber() {
        NativeMycatServer nativeMycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
        MySQLDatasourcePool sqlDatasourcePool = nativeMycatServer.getDatasource(targetName);
        return sqlDatasourcePool.currentSessionCount();
    }
}
