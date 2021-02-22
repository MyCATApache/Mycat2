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

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.*;

import java.text.MessageFormat;
import java.util.function.Function;
import java.util.function.Supplier;

public class LocalXaSqlConnection extends BaseXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalXaSqlConnection.class);
    volatile SqlConnection localSqlConnection = null;
    volatile String targetName;

    public LocalXaSqlConnection(Supplier<MySQLManager> mySQLManagerSupplier,
                                XaLog xaLog) {
        super(mySQLManagerSupplier, xaLog);
    }

    @Override
    public Future<Void> begin() {
        if (inTranscation) {
            return Future.failedFuture(new IllegalArgumentException("occur Nested transaction"));
        }
        inTranscation = true;
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> commit() {
        String curXid = xid;
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            return (Future.succeededFuture());
        }
        if (targetName != null && localSqlConnection != null && map.isEmpty()) {
            return localSqlConnection.query("commit;").execute()
                    .onSuccess(event -> inTranscation = false).mapEmpty();
        }
        if (targetName != null && inTranscation && localSqlConnection != null) {
            return super.commitXa((coordinatorLog) -> localSqlConnection.query(
                    "REPLACE INTO mycat.xa_log (xid) VALUES('" +curXid+ "')").execute().mapEmpty())
                    .compose((Function<Void, Future<Void>>) o -> localSqlConnection.query("commit;").execute().mapEmpty()).mapEmpty()
                    .onSuccess(event -> {
                        Future<SqlConnection> connection = mySQLManager().getConnection(targetName);
                        connection
                                .onSuccess(sqlConnection -> sqlConnection.query("delete from mycat.xa_log where xid = '" + curXid+"'")
                                        .execute()
                                        .onComplete(event1 -> sqlConnection.close()));
                        targetName = null;
                        localSqlConnection.close();
                        localSqlConnection = null;
                        inTranscation = false;
                    }).mapEmpty();
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public Future<SqlConnection> getConnection(String targetName) {
        MySQLManager mySQLManager = mySQLManager();
        if (inTranscation) {
            if (this.targetName == null && localSqlConnection == null) {
                LocalXaSqlConnection.this.targetName = targetName;
                Future<SqlConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);
                return sqlConnectionFuture.map(sqlConnection -> {
                    LocalXaSqlConnection.this.localSqlConnection = sqlConnection;
                    return sqlConnection;
                }).compose(sqlConnection -> sqlConnection.query("begin;").execute().map(sqlConnection));
            }
            if (this.targetName != null && this.targetName.equals(targetName)) {
                return Future.succeededFuture(localSqlConnection);
            }
            if (xid == null) {
                xid = log.nextXid();
                log.beginXa(xid);
            }
            return super.getConnection(targetName);
        }
        return mySQLManager.getConnection(targetName).map(connection -> {
            addCloseConnection(connection);
            return connection;
        });
    }

    @Override
    public Future<Void> rollback() {
        String curtargetName = targetName;
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            return Future.succeededFuture();
        }
        String curXid = this.xid;
        return Future.future(promise -> {
            localSqlConnection.query("rollback;").execute()
                    .onComplete(ignored -> {
                        localSqlConnection.close();
                        super.rollback().onSuccess(event -> {
                            Future<SqlConnection> connection = mySQLManager().getConnection(curtargetName);
                            connection
                                    .onSuccess(sqlConnection -> sqlConnection.query("delete from mycat.xa_log where xid = '" + curXid+"'")
                                            .execute()
                                            .onComplete(event1 -> sqlConnection.close()));
                        }).onComplete(promise);
                        localSqlConnection = null;
                        targetName = null;
                        inTranscation = false;
                    });
        });
    }

    @Override
    public Future<Void> closeStatementState() {
        return Future.future(promise -> {
            super.closeStatementState()
                    .onComplete(event -> {
                        if (!isInTransaction()) {
                            targetName = null;
                            SqlConnection localSqlConnection = this.localSqlConnection;
                            this.localSqlConnection = null;
                            if (localSqlConnection != null) {
                                localSqlConnection.close().onComplete(promise);
                            } else {
                                promise.tryComplete();
                            }
                        } else {
                            promise.tryComplete();
                        }
                    });
        });
    }

    @Override
    public Future<Void> close() {
        return super.close().flatMap(event -> {
            if (localSqlConnection != null) {
                return localSqlConnection
                        .query("rollback")
                        .execute()
                        .flatMap(c -> localSqlConnection.close()
                                .onComplete(event1 -> {
                                    localSqlConnection = null;
                                    targetName = null;
                                }));

            } else {
                return Future.succeededFuture();
            }
        });
    }
}
