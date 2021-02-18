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
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.SqlConnection;

import java.text.MessageFormat;
import java.util.function.Supplier;

public class LocalXaSqlConnection extends BaseXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalXaSqlConnection.class);
    volatile SqlConnection localSqlConnection = null;
    volatile String targetName;
    private final String LOCAL_XA_COMMIT_SQL;

    public LocalXaSqlConnection(Supplier<MySQLManager> mySQLManagerSupplier,
                                XaLog xaLog,
                                String schemaName,
                                String tableName) {
        super(mySQLManagerSupplier, xaLog);
        LOCAL_XA_COMMIT_SQL = MessageFormat.format(
                "REPLACE INTO {0}.{1} (xid,state,expires,info) VALUES ({0},{1},{2},{3});COMMIT;",
                schemaName, tableName);
    }

    protected String getLocalXACommitSQL(ImmutableCoordinatorLog log) {
        return MessageFormat.format(LOCAL_XA_COMMIT_SQL, log.getXid(), log.computeMinState(), log.computeExpires(), log.toJson());
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
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            return (Future.succeededFuture());
        }
        if (targetName != null && localSqlConnection != null && map.isEmpty()) {
            return localSqlConnection.query("commit;").execute()
                    .onSuccess(event -> inTranscation = false).mapEmpty();
        }
        if (targetName != null && inTranscation && localSqlConnection != null) {
            return super.commitXa((coordinatorLog) -> localSqlConnection.query(getLocalXACommitSQL(coordinatorLog)).execute().mapEmpty());
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
                    map.put(targetName, sqlConnection);
                    return sqlConnection;
                }).compose(sqlConnection -> sqlConnection.query("begin;").execute().map(sqlConnection));
            }
            if (this.targetName != null && this.targetName.equals(targetName)) {
                return Future.succeededFuture(localSqlConnection);
            }
            xid = log.nextXid();
            log.beginXa(xid);
            return super.getConnection(targetName);
        }
        return mySQLManager.getConnection(targetName).map(connection -> {
            addCloseConnection(connection);
            return connection;
        });
    }

    @Override
    public Future<Void> rollback() {
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            return Future.succeededFuture();
        }
        return Future.future(promise -> {
            localSqlConnection.query("rollback;").execute()
                    .onComplete(ignored -> {
                        super.rollback().onComplete(promise);
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
