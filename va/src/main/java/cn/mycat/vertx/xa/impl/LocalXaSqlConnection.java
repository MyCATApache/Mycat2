/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.SqlConnection;

public class LocalXaSqlConnection extends BaseXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalXaSqlConnection.class);
    volatile SqlConnection localSqlConnection = null;
    volatile String targetName;

    public LocalXaSqlConnection(MySQLManager mySQLManager, XaLog xaLog) {
        super(mySQLManager, xaLog);
    }

    @Override
    public void begin(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            handler.handle(Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
            return;
        }
        inTranscation = true;
        handler.handle(Future.succeededFuture());
    }

    @Override
    public void commit(Handler<AsyncResult<Void>> handler) {
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            handler.handle(Future.succeededFuture());
            return;
        }
        if (targetName != null && localSqlConnection != null && map.isEmpty()) {
            localSqlConnection.query("commit;").execute(event -> {
                if (event.succeeded()) {
                    inTranscation = false;
                    handler.handle(Future.succeededFuture());
                    return;
                }
                handler.handle(Future.failedFuture(event.cause()));
            });
            return;
        }
        if (targetName != null && inTranscation && localSqlConnection != null) {
            super.commitXa(()->{
              return localSqlConnection.query("commit;").execute();
            }, handler);
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public Future<SqlConnection> getConnection(String targetName) {
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
            xid = log.nextXid();
            log.beginXa(xid);
            return super.getConnection(targetName);
        }
        return mySQLManager.getConnection(targetName);
    }

    @Override
    public void rollback(Handler<AsyncResult<Void>> handler) {
        if (targetName == null && localSqlConnection == null && map.isEmpty()) {
            inTranscation = false;
            handler.handle(Future.succeededFuture());
            return;
        }
        localSqlConnection.query("rollback;").execute()
                .onComplete(ignored -> {
                    super.rollback(handler);
                });
    }

    @Override
    public void closeStatementState(Handler<AsyncResult<Void>> handler) {
        if (!isInTranscation()) {
            targetName = null;
            SqlConnection localSqlConnection = this.localSqlConnection;
            this.localSqlConnection = null;
            if (localSqlConnection!=null){
                localSqlConnection.close(event -> LocalXaSqlConnection.super.closeStatementState(handler));
            }
            return;
        }
        super.closeStatementState(handler);
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        if (localSqlConnection!=null){
            localSqlConnection.query("rollback").execute().onComplete(c->localSqlConnection.close());
            localSqlConnection = null;
            targetName = null;
        }
        super.close(handler);
    }
}
