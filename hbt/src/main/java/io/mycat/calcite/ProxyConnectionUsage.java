package io.mycat.calcite;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.api.collector.RowObservable;
import io.mycat.api.collector.SimpleRowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.apache.calcite.rel.RelNode;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ProxyConnectionUsage {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyConnectionUsage.class);
    private final MycatDataContext context;
    private final List<SQLKey> targets;
    private volatile static ExecutorService executorService = null;

    public ProxyConnectionUsage(MycatDataContext context, List<SQLKey> targets) {
        this.context = context;
        this.targets = targets;
    }

    ExecutorService getTrxExecutorService() {
        if (executorService == null) {
            synchronized (ProxyConnectionUsage.class) {
                if (executorService == null) {
                    executorService = Executors.newSingleThreadExecutor();
                }
            }
        }
        return executorService;
    }

    public Future<IdentityHashMap<RelNode, List<RowObservable>>> collect(XaSqlConnection xaconnection, List<Object> params) {
        IdentityHashMap<RelNode, List<RowObservable>> finalResMap = new IdentityHashMap<>();
        Map<RelNode, List<RowObservable>> resMap = Collections.synchronizedMap(finalResMap);
        if (context.isInTransaction()) {
            return getConnectionWhenTranscation(xaconnection)
                    .flatMap(new QueryResultSetInTranscation(xaconnection, resMap, params, finalResMap));
        } else {
            return getConnection(xaconnection).flatMap(new Function<Map<String, LinkedList<SqlConnection>>, Future<IdentityHashMap<RelNode, List<RowObservable>>>>() {
                @Override
                public Future<IdentityHashMap<RelNode, List<RowObservable>>> apply(Map<String, LinkedList<SqlConnection>> stringLinkedListMap) {
                    ArrayList<Future> objects = new ArrayList<>();
                    for (SQLKey target : targets) {
                        LinkedList<SqlConnection> sqlConnections = stringLinkedListMap.get(context.resolveDatasourceTargetName(target.getTargetName()));
                        SqlConnection sqlConnection = sqlConnections.pop();
                        Future<RowObservable> future = Future.succeededFuture(VertxExecuter.runQuery(Future.succeededFuture(sqlConnection), target.getSql().getSql(),
                                MycatPreparedStatementUtil.extractParams(params, target.getSql().getDynamicParameters())));
                        objects.add(future.map(rowObservable -> {
                            synchronized (resMap) {
                                List<RowObservable> rowObservables = resMap.computeIfAbsent(target.getMycatView(), node -> new LinkedList<>());
                                rowObservables.add(wrapAsAutoCloseConnectionRowObservale(sqlConnection, rowObservable));
                            }
                            return null;
                        }));
                    }
                    return CompositeFuture.all(objects).map(compositeFuture -> {
                        return finalResMap;
                    });
                }
            });
        }
    }

    @NotNull
    public static RowObservable wrapAsAutoCloseConnectionRowObservale(SqlConnection sqlConnection, RowObservable rowObservable) {
        return new RowObservable() {
            @Override
            public MycatRowMetaData getRowMetaData() {
                return rowObservable.getRowMetaData();
            }

            @Override
            protected void subscribeActual(@NonNull Observer<? super Object[]> observer) {
                rowObservable.subscribe(new Observer<Object[]>() {
                    @Override
                    public void onSubscribe(@NonNull Disposable d) {
                        observer.onSubscribe(d);
                    }

                    @Override
                    public void onNext(Object @NonNull [] objects) {
                        LOGGER.debug(Arrays.stream(objects).filter(i->i!=null).map(i->i.getClass().toString())
                                .collect(Collectors.joining(",")));
                        LOGGER.debug(Arrays.toString(objects));

                        observer.onNext(objects);
                    }

                    @Override
                    public void onError(@NonNull Throwable e) {
                        observer.onError(e);
                    }

                    @Override
                    public void onComplete() {
                        sqlConnection.close();
                        observer.onComplete();
                    }
                });
            }
        };
    }

    private Future<Void> getResultSet(Map<RelNode, List<RowObservable>> resMap,
                                      SqlConnection connection2,
                                      List<SQLKey> res,
                                      List<Object> params) {
        Future<Void> future = Future.succeededFuture();
        if (res.size() > 1) {
            for (SQLKey sqlKey : res.subList(1, res.size())) {
                future = future.flatMap(new GetResultSetAndCache(connection2, sqlKey, params, resMap));
            }
        }
        return future.flatMap(new GetResultSetEnd(res, connection2, params, resMap));
    }

    private Future<Map<String, LinkedList<SqlConnection>>> getConnection(XaSqlConnection connection) {

        List<String> strings = targets.stream().map(i ->
                context.resolveDatasourceTargetName(i.getTargetName())).collect(Collectors.toList());
        Map<String, LinkedList<SqlConnection>> map = new ConcurrentHashMap<>();

        PromiseInternal<Map<String, LinkedList<SqlConnection>>> promise = VertxUtil.newPromise();
         getTrxExecutorService().submit(new SequenceTask(strings, connection, map, promise));
        return promise.future();
    }

    private Future<Map<String, List<SQLKey>>> getConnectionWhenTranscation(XaSqlConnection connection) {
        Map<String, List<SQLKey>> map = targets.stream().collect(Collectors.groupingBy(i ->
                context.resolveDatasourceTargetName(i.getTargetName(), context.isInTransaction())));
        TransactionSession transactionSession = context.getTransactionSession();
        PromiseInternal<Map<String, List<SQLKey>>> promise = VertxUtil.newPromise();
        getTrxExecutorService().submit(new SequenceTranscationTask(map, connection, transactionSession, promise));
        return promise;
    }

    private static class SequenceTranscationTask implements Callable<Future<? extends Object>> {
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
        public Future<? extends Object> call() throws Exception {
            try {
                Future<Void> future = Future.succeededFuture();
                for (String s : map.keySet()) {
                    future = future
                            .flatMap(unused -> {
                                return (Future) connection.getConnection(transactionSession.resolveFinalTargetName(s, true));
                            });
                }
                return future.onComplete(event -> {
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
            return promise;
        }
    }

    private static class GetResultSetAndCache implements Function<Void, Future<Void>> {
        private final SqlConnection connection2;
        private final SQLKey sqlKey;
        private final List<Object> params;
        private final Map<RelNode, List<RowObservable>> resMap;

        public GetResultSetAndCache(SqlConnection connection2, SQLKey sqlKey, List<Object> params, Map<RelNode, List<RowObservable>> resMap) {
            this.connection2 = connection2;
            this.sqlKey = sqlKey;
            this.params = params;
            this.resMap = resMap;
        }

        @Override
        public Future<Void> apply(Void unused) {
            Future<RowObservable> rowObservableFuture = Future.succeededFuture(VertxExecuter.runQuery(
                    Future.succeededFuture(connection2),
                    sqlKey.getSql().getSql(),
                    MycatPreparedStatementUtil.extractParams(params, sqlKey.getSql().getDynamicParameters())
            ));
            return rowObservableFuture.flatMap(rowObservable -> {
                PromiseInternal<Void> promise = VertxUtil.newPromise();
                ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                rowObservable.subscribe(objects -> builder.add(objects),
                        throwable -> promise.tryFail(throwable),
                        () -> {
                            RowObservable observable = SimpleRowObservable.of(rowObservable.getRowMetaData(), builder.build());
                            synchronized (resMap) {
                                List<RowObservable> rowObservables = resMap.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
                                rowObservables.add(observable);
                            }
                            promise.tryComplete();
                        });
                return promise.future();
            });
        }
    }

    private  class SequenceTask implements Callable<Future<?>> {

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
        public Future<?> call() throws Exception {
            try {
                Future<Void> future = Future.succeededFuture();
                for (String string : strings) {
                    future = future.flatMap(unused -> {
                        Future<SqlConnection> connection1 = connection.getConnection(string);
                        return connection1.map(new AddConnection(string));
                    });
                }
                return future.onComplete(event -> {
                    if (event.succeeded()) {
                        if(LOGGER.isDebugEnabled()){
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
            return promise;
        }

        private class AddConnection implements Function<SqlConnection, Void> {
            private final String string;

            public AddConnection(String string) {
                this.string = string;

            }

            @Override
            public Void apply(SqlConnection i) {
                if(LOGGER.isDebugEnabled()){
                    LOGGER.debug("AddConnection callback");
                }
                synchronized (ProxyConnectionUsage.this) {
                    LinkedList<SqlConnection> defaultConnections = map.computeIfAbsent(string, s -> new LinkedList<>());
                    defaultConnections.add(i);
                }
                return null;
            }
        }
    }

    private class QueryResultSetInTranscation implements Function<Map<String, List<SQLKey>>, Future<IdentityHashMap<RelNode, List<RowObservable>>>> {
        private final XaSqlConnection xaconnection;
        private final Map<RelNode, List<RowObservable>> resMap;
        private final List<Object> params;
        private final IdentityHashMap<RelNode, List<RowObservable>> finalResMap;

        public QueryResultSetInTranscation(XaSqlConnection xaconnection, Map<RelNode, List<RowObservable>> resMap, List<Object> params, IdentityHashMap<RelNode, List<RowObservable>> finalResMap) {
            this.xaconnection = xaconnection;
            this.resMap = resMap;
            this.params = params;
            this.finalResMap = finalResMap;
        }

        @Override
        public Future<IdentityHashMap<RelNode, List<RowObservable>>> apply(Map<String, List<SQLKey>> stringListMap) {
            ArrayList<Map.Entry<String, List<SQLKey>>> entries = new ArrayList<>(stringListMap.entrySet());
            ArrayList<Future> resList = new ArrayList<>();
            for (Map.Entry<String, List<SQLKey>> stringListEntry : entries) {
                Future<SqlConnection> connection1 = xaconnection.getConnection(
                        context.resolveDatasourceTargetName(stringListEntry.getKey(), true));
                resList.add(connection1.flatMap(connection2 -> {
                    return ProxyConnectionUsage.this.getResultSet(resMap, connection2, stringListEntry.getValue(), params);
                }));
            }
            return CompositeFuture.all(resList).map(compositeFuture -> finalResMap);
        }
    }

    private class GetResultSetEnd implements Function<Void, Future<Void>> {
        private final List<SQLKey> res;
        private final SqlConnection connection2;
        private final List<Object> params;
        private final Map<RelNode, List<RowObservable>> resMap;

        public GetResultSetEnd(List<SQLKey> res, SqlConnection connection2, List<Object> params, Map<RelNode, List<RowObservable>> resMap) {
            this.res = res;
            this.connection2 = connection2;
            this.params = params;
            this.resMap = resMap;
        }

        @Override
        public Future<Void> apply(Void unused) {
            SQLKey sqlKey = res.get(0);
            Future<RowObservable> rowObservableFuture = Future.succeededFuture(VertxExecuter.runQuery(
                    Future.succeededFuture(connection2),
                    sqlKey.getSql().getSql(),
                    MycatPreparedStatementUtil.extractParams(params, sqlKey.getSql().getDynamicParameters())));
            return rowObservableFuture.map(rowObservable -> {
                synchronized (ProxyConnectionUsage.this) {
                    List<RowObservable> rowObservables = resMap.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
                    rowObservables.add(rowObservable);
                }
                return null;
            });
        }
    }
}
