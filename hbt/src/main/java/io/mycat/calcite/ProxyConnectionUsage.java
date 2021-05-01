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
package io.mycat.calcite;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.apache.calcite.rel.RelNode;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ProxyConnectionUsage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyConnectionUsage.class);
    private final MycatDataContext context;
    private final List<SQLKey> targets;
    private volatile static java.util.concurrent.Executor executorService = null;

    public ProxyConnectionUsage(MycatDataContext context, List<SQLKey> targets) {
        this.context = context;
        this.targets = targets;
    }

    java.util.concurrent.Executor getTrxExecutorService() {
        if (executorService == null) {
            synchronized (ProxyConnectionUsage.class) {
                if (executorService == null) {
                    executorService = new java.util.concurrent.Executor() {

                        @Override
                        public synchronized void execute(@NotNull Runnable command) {
                            command.run();
                        }
                    };
                }
            }
        }
        return executorService;
    }

    public Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>> collect(XaSqlConnection xaconnection, List<Object> params) {
        IdentityHashMap<RelNode, List<Observable<Object[]>>> finalResMap = new IdentityHashMap<>();
        Map<RelNode, List<Observable<Object[]>>> resMap = Collections.synchronizedMap(finalResMap);
        if (context.isInTransaction() != context.getTransactionSession().isInTransaction()) {
            throw new IllegalArgumentException("the isInTransaction state not sync");
        }
        if (context.isInTransaction()) {
            return getConnectionWhenTranscation(xaconnection)
                    .flatMap(new QueryResultSetInTranscation(xaconnection, resMap, params, finalResMap));
        } else {
            return getConnection(xaconnection).flatMap(stringLinkedListMap -> {
                try {
                    for (SQLKey target : targets) {
                        LinkedList<SqlConnection> sqlConnections = stringLinkedListMap.get(context.resolveDatasourceTargetName(target.getTargetName()));
                        SqlConnection sqlConnection = sqlConnections.pop();
                        Promise<SqlConnection> closePromise = VertxUtil.newPromise();
                        xaconnection.addCloseFuture(closePromise.future());
                        Observable<Object[]> observable = VertxExecuter.runQuery(Future.succeededFuture(sqlConnection),
                                target.getSql().getSql(),
                                MycatPreparedStatementUtil.extractParams(params, target.getSql().getDynamicParameters()),
                                target.getRowMetaData())
                                .doOnTerminate(() -> closePromise.tryComplete(sqlConnection));
                        synchronized (resMap) {
                            List<Observable<Object[]>> rowObservables = resMap.computeIfAbsent(target.getMycatView(), node -> new LinkedList<>());
                            rowObservables.add(observable);
                        }
                    }
                } catch (Throwable throwable) {
                    return Future.failedFuture(throwable);
                }
                return Future.succeededFuture(finalResMap);
            });
        }
    }

    private Future<Void> getResultSet(XaSqlConnection xaconnection, Map<RelNode, List<Observable<Object[]>>> resMap,
                                      SqlConnection sqlConnection,
                                      List<SQLKey> res,
                                      List<Object> params) {
        Future<Void> future = Future.succeededFuture();
        if (res.size() > 1) {
            for (SQLKey sqlKey : res.subList(1, res.size())) {
                future = future.flatMap(new GetResultSetAndCache(xaconnection, sqlConnection, sqlKey, params, resMap));
            }
        }
        return future.flatMap(new GetResultSetEnd(res.get(0), xaconnection, sqlConnection, params, resMap));
    }

    private Future<Map<String, LinkedList<SqlConnection>>> getConnection(XaSqlConnection connection) {

        List<String> strings = targets.stream().map(i ->
                context.resolveDatasourceTargetName(i.getTargetName())).collect(Collectors.toList());
        Map<String, LinkedList<SqlConnection>> map = new ConcurrentHashMap<>();

        PromiseInternal<Map<String, LinkedList<SqlConnection>>> promise = VertxUtil.newPromise();
        getTrxExecutorService().execute(new SequenceTask(strings, connection, map, promise));
        return promise.future();
    }

    private Future<Map<String, List<SQLKey>>> getConnectionWhenTranscation(XaSqlConnection connection) {
        Map<String, List<SQLKey>> map = targets.stream().collect(Collectors.groupingBy(i ->
                context.resolveDatasourceTargetName(i.getTargetName(), context.isInTransaction())));
        TransactionSession transactionSession = context.getTransactionSession();
        PromiseInternal<Map<String, List<SQLKey>>> promise = VertxUtil.newPromise();
        getTrxExecutorService().execute(new SequenceTranscationTask(map, connection, transactionSession, promise));
        return promise;
    }

    private static class SequenceTranscationTask implements Runnable {
        private final Map<String, List<SQLKey>> map;
        private final XaSqlConnection connection;
        private final TransactionSession transactionSession;
        private final PromiseInternal<Map<String, List<SQLKey>>> promise;

        public SequenceTranscationTask(Map<String, List<SQLKey>> map, XaSqlConnection connection, TransactionSession transactionSession, PromiseInternal<Map<String, List<SQLKey>>> promise) {
            this.map = map;
            this.connection = connection;
            this.transactionSession = transactionSession;
            this.promise = promise;
        }


        @Override
        public void run() {
            try {
                Future<Void> future = Future.succeededFuture();

                for (String s : map.keySet()) {
                    future = future
                            .flatMap(unused -> {
                                return (Future) connection.getConnection(transactionSession.resolveFinalTargetName(s, true)).mapEmpty();
                            });
                }
                future.onComplete(event -> {
                    if (event.succeeded()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("get getConnectionWhenTranscation");
                        }
                        promise.tryComplete(map);
                    } else {
                        promise.fail(event.cause());
                    }
                });
            } catch (Throwable throwable) {
                promise.tryFail(throwable);
            }
        }
    }

    private static class GetResultSetAndCache implements Function<Void, Future<Void>> {
        private XaSqlConnection xaconnection;
        private final SqlConnection sqlConnection;
        private final SQLKey sqlKey;
        private final List<Object> params;
        private final Map<RelNode, List<Observable<Object[]>>> resMap;

        public GetResultSetAndCache(XaSqlConnection xaconnection, SqlConnection connection,
                                    SQLKey sqlKey, List<Object> params,
                                    Map<RelNode, List<Observable<Object[]>>> resMap) {
            this.xaconnection = xaconnection;
            this.sqlConnection = connection;
            this.sqlKey = sqlKey;
            this.params = params;
            this.resMap = resMap;
        }

        @Override
        public Future<Void> apply(Void unused) {
            Future<Observable<Object[]>> rowObservableFuture = Future.succeededFuture(VertxExecuter.runQuery(
                    Future.succeededFuture(sqlConnection),
                    sqlKey.getSql().getSql(),
                    MycatPreparedStatementUtil.extractParams(params, sqlKey.getSql().getDynamicParameters()),
                    sqlKey.getRowMetaData()
            ));
            return rowObservableFuture.flatMap(rowObservable -> {
                PromiseInternal<SqlConnection> closePromise = VertxUtil.newPromise();
                xaconnection.addCloseFuture(closePromise.future());
                rowObservable = rowObservable.doAfterTerminate(() -> {
                    closePromise.tryComplete(sqlConnection);
                });
                Promise<Void> promise = VertxUtil.newPromise();
                ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                rowObservable.subscribe(objects -> builder.add(objects),
                        throwable -> promise.tryFail(throwable),
                        () -> {
                            Observable<Object[]> observable = Observable.fromIterable(builder.build());
                            synchronized (resMap) {
                                List<Observable<Object[]>> rowObservables = resMap.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
                                rowObservables.add(observable);
                            }
                            promise.tryComplete();
                        });
                return promise.future();
            });
        }
    }

    private class SequenceTask implements Runnable {

        private final List<String> strings;
        private final XaSqlConnection connection;
        private final Map<String, LinkedList<SqlConnection>> map;
        private final PromiseInternal<Map<String, LinkedList<SqlConnection>>> promise;

        public SequenceTask(List<String> strings,
                            XaSqlConnection connection,
                            Map<String, LinkedList<SqlConnection>> map,
                            PromiseInternal<Map<String, LinkedList<SqlConnection>>> promise) {
            this.strings = strings;
            this.connection = connection;
            this.map = map;
            this.promise = promise;
        }

        @Override
        public void run() {
            try {
                Future<Void> future = Future.succeededFuture();
                for (String string : strings) {
                    future = future.flatMap(unused -> {
                        Future<SqlConnection> connection1 = connection.getConnection(string);
                        return connection1.map(new AddConnection(string, this.connection));
                    });
                }
                future.onComplete(event -> {
                    if (event.succeeded()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("SequenceTask finished");
                        }
                        promise.tryComplete(map);
                    } else {
                        promise.fail(event.cause());
                    }
                });
            } catch (Throwable throwable) {
                promise.fail(throwable);
            }
        }

        private class AddConnection implements Function<SqlConnection, Void> {
            private final String string;

            public AddConnection(String string, XaSqlConnection connection) {
                this.string = string;

            }

            @Override
            public Void apply(SqlConnection i) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("AddConnection callback");
                }
                synchronized (ProxyConnectionUsage.this) {
                    LinkedList<SqlConnection> defaultConnections = map.computeIfAbsent(string, s -> new LinkedList<>());
                    defaultConnections.add(i);
                    if (!connection.isInTransaction()) {
                        connection.addCloseConnection(i);
                    }
                }
                return null;
            }
        }
    }

    private class QueryResultSetInTranscation implements Function<Map<String, List<SQLKey>>,
            Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>>> {
        private final XaSqlConnection xaconnection;
        private final Map<RelNode, List<Observable<Object[]>>> resMap;
        private final List<Object> params;
        private final IdentityHashMap<RelNode, List<Observable<Object[]>>> finalResMap;

        public QueryResultSetInTranscation(XaSqlConnection xaconnection,
                                           Map<RelNode, List<Observable<Object[]>>> resMap,
                                           List<Object> params,
                                           IdentityHashMap<RelNode,
                                                   List<Observable<Object[]>>> finalResMap) {
            this.xaconnection = xaconnection;
            this.resMap = resMap;
            this.params = params;
            this.finalResMap = finalResMap;
        }

        @Override
        public Future<IdentityHashMap<RelNode, List<Observable<Object[]>>>> apply(Map<String, List<SQLKey>> stringListMap) {
            ArrayList<Map.Entry<String, List<SQLKey>>> entries = new ArrayList<>(stringListMap.entrySet());
            ArrayList<Future> resList = new ArrayList<>();
            for (Map.Entry<String, List<SQLKey>> stringListEntry : entries) {
                Future<SqlConnection> connection1 = xaconnection.getConnection(
                        context.resolveDatasourceTargetName(stringListEntry.getKey(), true));
                resList.add(connection1.flatMap(sqlConnection -> {
                    return ProxyConnectionUsage.this.getResultSet(xaconnection, resMap,
                            sqlConnection,
                            stringListEntry.getValue(),
                            params);
                }));
            }
            return CompositeFuture.all(resList).map(compositeFuture -> finalResMap);
        }
    }

    private class GetResultSetEnd implements Function<Void, Future<Void>> {
        private final SQLKey sqlKey;
        private final XaSqlConnection xaconnection;
        private final SqlConnection sqlConnection;
        private final List<Object> params;
        private final Map<RelNode, List<Observable<Object[]>>> resMap;

        public GetResultSetEnd(SQLKey sqlKey, XaSqlConnection xaconnection, SqlConnection sqlConnection, List<Object> params, Map<RelNode, List<Observable<Object[]>>> resMap) {
            this.sqlKey = sqlKey;
            this.xaconnection = xaconnection;
            this.sqlConnection = sqlConnection;
            this.params = params;
            this.resMap = resMap;
        }

        @Override
        public Future<Void> apply(Void unused) {
            Future<Observable<Object[]>> rowObservableFuture = Future.succeededFuture(VertxExecuter.runQuery(
                    Future.succeededFuture(sqlConnection),
                    sqlKey.getSql().getSql(),
                    MycatPreparedStatementUtil.extractParams(params, sqlKey.getSql().getDynamicParameters()),
                    sqlKey.getRowMetaData()));
            return rowObservableFuture.map(rowObservable -> {
                Promise<SqlConnection> closePromise = VertxUtil.newPromise();
                xaconnection.addCloseFuture(closePromise.future());
                synchronized (ProxyConnectionUsage.this) {
                    List<Observable<Object[]>> rowObservables = resMap.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
                    rowObservables.add(rowObservable.doOnTerminate(() -> closePromise.tryComplete(sqlConnection)));
                }
                return null;
            });
        }
    }
}
