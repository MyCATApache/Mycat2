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

import cn.mycat.vertx.xa.XaLog;
import cn.mycat.vertx.xa.XaSqlConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.SqlConnection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractXaSqlConnection implements XaSqlConnection {
    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractXaSqlConnection.class);
    protected boolean autocommit = true;
    protected boolean inTranscation = false;
    protected final XaLog log;
    protected List<SqlConnection> closeConnections = Collections.synchronizedList(
            new ArrayList<>());

    public AbstractXaSqlConnection(XaLog xaLog) {
        this.log = xaLog;
    }

    @Override
    public boolean isAutocommit() {
        return autocommit;
    }

    @Override
    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    /**
     * a sql runs before call it,if autocommit = false,it should begin a transcation.
     *
     * @param handler the callback handler
     */
    @Override
    public void openStatementState(Handler<AsyncResult<Void>> handler) {
        Future<Void> future = openStatementState();
        if (handler != null) {
            future.onComplete(handler);
        }
    }

    public Future<Void> openStatementState() {
        if (!isAutocommit()) {
            if (!isInTransaction()) {
                return begin();
            }
        }
        return Future.succeededFuture();
    }

    @Override
    public boolean isInTransaction() {
        return inTranscation;
    }


    public Future<Void> dealCloseConnections() {
        List<Future> futures = closeConnections.stream().map(c -> c.close()).collect(Collectors.toList());
        return CompositeFuture.all(futures)
                .onComplete(event -> closeConnections.clear()).mapEmpty();
    }

    @Override
    public Future<Void> closeStatementState() {
        return dealCloseConnections();
    }

    @Override
    public void addCloseConnection(SqlConnection sqlConnection) {
        closeConnections.add(sqlConnection);
    }
    public void closeStatementState(Handler<AsyncResult<Void>> handler){
        Future<Void> future = closeStatementState();
        if (handler!=null){
            future.onComplete(handler);
        }
    }
}
