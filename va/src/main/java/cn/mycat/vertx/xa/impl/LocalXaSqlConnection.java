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
        if (localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            return (Future.succeededFuture());
        }
        if (localSqlConnection != null && map.isEmpty()) {
            return localSqlConnection.query("commit;").execute()
                    .onSuccess(event -> inTranscation = false).mapEmpty();
        }
        if (inTranscation && localSqlConnection != null) {
            return super.commitXa((coordinatorLog) -> localSqlConnection.query(
                    "REPLACE INTO mycat.xa_log (xid) VALUES('" + curXid + "')").execute().mapEmpty())
                    .compose((Function<Void, Future<Void>>) o -> localSqlConnection.query("commit;").execute().mapEmpty()).mapEmpty()
                    .compose(o -> {
                        xid = null;
                        inTranscation = false;
                        SqlConnection localSqlConnection = this.localSqlConnection;
                        this.localSqlConnection = null;
                        return localSqlConnection.close();
                    })
                    .onSuccess(event -> {
                        if (targetName != null) {
                            Future<SqlConnection> connectionFuture = mySQLManager().getConnection(targetName);
                            connectionFuture
                                    .onSuccess(sqlConnection -> sqlConnection.query("delete from mycat.xa_log where xid = '" + curXid + "'")
                                            .execute()
                                            .onComplete(event1 -> connectionFuture.map(c -> c.close())));
                        }
                        targetName = null;
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
                }).compose(sqlConnection -> sqlConnection
                        .query(getTransactionIsolation().getCmd()).execute()
                        .mapEmpty()
                        .flatMap(unused -> sqlConnection.query("begin;").execute().map(sqlConnection)));
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
        return super.rollback().flatMap(unused -> localSqlConnection.query("rollback;")
                .execute().eventually(unused1 -> {
                    SqlConnection curLocalSqlConnection = this.localSqlConnection;
                    this.localSqlConnection = null;
                    targetName = null;
                    xid = null;
                    if (curXid != null) {
                        Future<SqlConnection> sqlConnectionFuture = Future.succeededFuture(curLocalSqlConnection);
                        return sqlConnectionFuture
                                .onSuccess(sqlConnection -> sqlConnection.query("delete from mycat.xa_log where xid = '" + curXid + "'")
                                        .execute()
                                        .onComplete(event1 -> sqlConnectionFuture.map(c -> c.close()))).mapEmpty();
                    }else {
                        return curLocalSqlConnection.close();
                    }
                }).mapEmpty());
    }

    @Override
    public Future<Void> closeStatementState() {
        return super.closeStatementState()
                .eventually(event -> {
                    if (!isInTransaction()) {
                        SqlConnection localSqlConnection = this.localSqlConnection;
                        this.localSqlConnection = null;
                        this.targetName = null;
                        if (localSqlConnection != null) {
                            return localSqlConnection.close();
                        } else {
                            return Future.succeededFuture();
                        }
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    @Override
    public Future<Void> close() {
        return super.close().eventually(event -> {
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
