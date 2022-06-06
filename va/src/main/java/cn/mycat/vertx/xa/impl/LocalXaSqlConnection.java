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

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class LocalXaSqlConnection extends BaseXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalXaSqlConnection.class);
    volatile NewMycatConnection localSqlConnection = null;

    public LocalXaSqlConnection(MySQLIsolation isolation, Supplier<MySQLManager> mySQLManagerSupplier,
                                XaLog xaLog) {
        super(isolation, mySQLManagerSupplier, xaLog);
    }

    @Override
    public Future<Void> begin() {
        if (inTranscation) {
            LOGGER.debug("local xa transaction occur nested transaction,xid:" + getXid());
            return Future.succeededFuture();
        }
        inTranscation = true;
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> commit() {
        /////////////////////////check///////////////////////
        if (!isInTransaction()) {
            if (localSqlConnection == null && map.isEmpty()) {
                //ok
            } else {
                LOGGER.error("meet commit in transaction bug");
            }
        }
        /////////////////////////check///////////////////////
        String curXid = xid;
        if (localSqlConnection == null && map.isEmpty()) {

            inTranscation = false;
            xid = null;
            localSqlConnection = null;
            return (Future.succeededFuture());
        }
        if (localSqlConnection != null && map.isEmpty()) {
            return localSqlConnection.update("commit;")
                    .transform(event -> {
                        Future<Void> closeFuture = localSqlConnection.close();

                        inTranscation = false;
                        xid = null;
                        localSqlConnection = null;

                        return closeFuture;
                    }).mapEmpty();
        }
        if (inTranscation && localSqlConnection != null) {
            NewMycatConnection curLocalSqlConnection = localSqlConnection;
            return super.commitXa((coordinatorLog) -> curLocalSqlConnection.update(
                            "REPLACE INTO mycat.xa_log (xid) VALUES('" + curXid + "');").mapEmpty())
                    .compose((Function<Void, Future<Void>>) o -> {
                        return curLocalSqlConnection.update("commit;").compose(unused -> {
                            return curLocalSqlConnection.update("delete from mycat.xa_log where xid = '" + curXid + "'").mapEmpty();
                        });
                    }).mapEmpty()
                    .transform(o -> {
                        if (o.failed()){
                            LOGGER.error("xa commit occurs exception",o.cause());
                        }
                        Future<Void> closeFuture = curLocalSqlConnection.close();

                        inTranscation = false;
                        xid = null;
                        localSqlConnection = null;
                        return closeFuture;
                    }).mapEmpty();
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public Future<NewMycatConnection> getConnection(String targetName) {
        MySQLManager mySQLManager = mySQLManager();
        if (inTranscation) {
            if (localSqlConnection == null) {
                Future<NewMycatConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);
                return sqlConnectionFuture.map(sqlConnection -> {
                    LocalXaSqlConnection.this.localSqlConnection = sqlConnection;
                    return sqlConnection;
                }).compose(sqlConnection -> sqlConnection
                        .update(getTransactionIsolation().getCmd())
                        .flatMap(unused -> sqlConnection.update("begin;").map(sqlConnection)));
            }
            if (this.localSqlConnection != null && this.localSqlConnection.getTargetName().equals(targetName)) {
                if (this.localSqlConnection.isClosed()) {
                    LOGGER.error("localSqlConnection is closed ,in transaction");
                }
                return Future.succeededFuture(localSqlConnection);
            }
            if (xid == null) {
                xid = log.nextXid();
                log.beginXa(xid);
            }
            return super.getConnection(targetName);
        }
        return mySQLManager.getConnection(targetName).map(connection -> {
            extraConnections.add(connection);
            return connection;
        });
    }

    @Override
    public Future<Void> rollback() {
        if (localSqlConnection == null && map.isEmpty()) {

            inTranscation = false;
            xid = null;
            localSqlConnection = null;
            return Future.succeededFuture();
        }
        if (localSqlConnection != null && map.isEmpty()) {
            return localSqlConnection.update("rollback;").transform(unused -> {
                LOGGER.error("", unused.cause());
                localSqlConnection.abandonConnection();

                inTranscation = false;
                xid = null;
                localSqlConnection = null;
                return Future.succeededFuture();
            }).mapEmpty();
        }
        String curXid = this.xid;
        NewMycatConnection curLocalSqlConnection = this.localSqlConnection;
        return super.rollback().compose(unused -> curLocalSqlConnection.update("rollback;").flatMap(unused1 -> {
            return curLocalSqlConnection.update("delete from mycat.xa_log where xid = '" + curXid + "'");
        })).transform(u -> {
            if (u.failed()) {
                LOGGER.error("", u.cause());
                if (curLocalSqlConnection != null) {
                    curLocalSqlConnection.abandonConnection();
                }
            } else {
                if (curLocalSqlConnection != null) {
                    curLocalSqlConnection.close();
                }
            }
            inTranscation = false;
            xid = null;
            localSqlConnection = null;
            return Future.succeededFuture();
        }).mapEmpty();
    }

    @Override
    public Future<Void> closeStatementState() {
        ///////////////////////////////check////////////////////////////////
        if (this.localSqlConnection != null) {
            if (inTranscation && this.localSqlConnection.isClosed()) {
                LOGGER.error("localSqlConnection is closed,in transaction");
            }
            if (inTranscation && !this.localSqlConnection.isClosed()) {
                //ok
            }
            if (!inTranscation && this.localSqlConnection.isClosed()) {
                LOGGER.error("localSqlConnection is closed,not in transaction");
                //fix
                this.localSqlConnection = null;
            }
            if (!inTranscation && !this.localSqlConnection.isClosed()) {
                LOGGER.error("localSqlConnection is not closed,not in transaction");
                //fix
                this.localSqlConnection.close();
                this.localSqlConnection = null;
            }
        }
        if (!map.isEmpty()) {
            if (inTranscation) {
                //ok
            }
            if (!inTranscation) {
                LOGGER.error("xa connection map is not empty ,in transaction");
            }
        }
        ///////////////////////////////check////////////////////////////////
        Future<Void> future = Future.succeededFuture();
        if (localSqlConnection != null) {
            future = localSqlConnection.abandonQuery();
        }
        return CompositeFuture.join(future, super.closeStatementState()
                .flatMap(event -> {
                    Future<Void> closeFuture = Future.succeededFuture();
                    if (!isInTransaction()) {
                        closeFuture = Optional.ofNullable(this.localSqlConnection)
                                .map(newMycatConnection -> localSqlConnection.close())
                                .orElse(Future.succeededFuture());
                        this.localSqlConnection = null;
                    }
                    return closeFuture;
                })).mapEmpty();
    }

    @Override
    public Future<Void> close() {
        return rollback();
    }

    @Override
    public List<NewMycatConnection> getExistedTranscationConnections() {
        if (localSqlConnection == null) {
            List<NewMycatConnection> existedTranscationConnections = super.getExistedTranscationConnections();
            if (!existedTranscationConnections.isEmpty()) {
                LOGGER.error("localSqlConnection is null but existed transcation connections is not empty");
            }
            return existedTranscationConnections;
        }
        ArrayList<NewMycatConnection> newMycatConnections = new ArrayList<>();
        newMycatConnections.add(localSqlConnection);
        newMycatConnections.addAll(super.getExistedTranscationConnections());
        return newMycatConnections;
    }

    @Override
    public Future<Void> kill() {
        Future<Void> future = rollback();
        return future.transform(unused -> {
            if (localSqlConnection != null) {
                localSqlConnection.abandonConnection();
                localSqlConnection = null;
            }
            return super.kill();
        });
    }

    @Override
    public List<NewMycatConnection> getAllConnections() {
        List<NewMycatConnection> allConnections = super.getAllConnections();
        ArrayList<NewMycatConnection> resList = new ArrayList<>(allConnections.size() + 1);
        if (localSqlConnection != null) {
            resList.add(localSqlConnection);
        }
        return resList;
    }
}
