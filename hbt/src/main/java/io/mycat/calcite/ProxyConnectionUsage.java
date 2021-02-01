package io.mycat.calcite;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.api.collector.RowObservable;
import io.mycat.api.collector.SimpleRowObservable;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.apache.calcite.rel.RelNode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


public class ProxyConnectionUsage {
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

    public Future<IdentityHashMap<RelNode, List<RowObservable>>> collect(XaSqlConnection connection, List<Object> params) {
        IdentityHashMap<RelNode, List<RowObservable>> finalResMap = new IdentityHashMap<>();
        Map<RelNode, List<RowObservable>> resMap = Collections.synchronizedMap(finalResMap);
        if (context.isInTransaction()) {
            return getConnectionWhenTranscation(connection).flatMap(stringListMap -> {
                ArrayList<Map.Entry<String, List<SQLKey>>> entries = new ArrayList<>(stringListMap.entrySet());
                ArrayList<Future> resList = new ArrayList<>();
                for (Map.Entry<String, List<SQLKey>> stringListEntry : entries) {
                    Future<SqlConnection> connection1 = connection.getConnection(
                            context.resolveDatasourceTargetName(stringListEntry.getKey(), true));
                    resList.add(connection1.flatMap(connection2 -> extracted(resMap, connection2, stringListEntry.getValue(), params)));
                }
                return CompositeFuture.all(resList).map(compositeFuture -> finalResMap);
            });
        } else {
            return getConnection(connection).flatMap(stringLinkedListMap -> {
                ArrayList<Future> objects = new ArrayList<>();
                for (SQLKey target : targets) {
                    LinkedList<SqlConnection> sqlConnections = stringLinkedListMap.get(context.resolveDatasourceTargetName(target.getTargetName()));
                    SqlConnection sqlConnection = sqlConnections.pop();
                    Future<RowObservable> future = VertxExecuter.runQuery(Future.succeededFuture(sqlConnection), target.getSql().getSql(),
                            MycatPreparedStatementUtil.extractParams(params, target.getSql().getDynamicParameters()));
                    objects.add(future.map(rowObservable -> {
                        synchronized (resMap) {
                            List<RowObservable> rowObservables = resMap.computeIfAbsent(target.getMycatView(), node -> new LinkedList<>());
                            rowObservables.add(rowObservable);
                        }
                        return null;
                    }));
                }
                return CompositeFuture.all(objects).map(compositeFuture -> finalResMap);
            });
        }
    }

    private Future<Void> extracted(Map<RelNode, List<RowObservable>> resMap,
                                   SqlConnection connection2,
                                   List<SQLKey> res,
                                   List<Object> params) {
        Future<Void> future = Future.succeededFuture();
        if (res.size() > 1) {
            for (SQLKey sqlKey : res.subList(1, res.size())) {
                future = future.flatMap(unused -> {
                    Future<RowObservable> rowObservableFuture = VertxExecuter.runQuery(
                            Future.succeededFuture(connection2),
                            sqlKey.getSql().getSql(),
                            MycatPreparedStatementUtil.extractParams(params, sqlKey.getSql().getDynamicParameters())
                    );
                    return rowObservableFuture.flatMap(rowObservable -> {
                        PromiseInternal<Void> promise = VertxUtil.newPromise();
                        ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                        rowObservable.subscribe(objects -> builder.add(objects),
                                throwable -> promise.fail(throwable),
                                () -> {
                                    RowObservable observable = SimpleRowObservable.of(rowObservable.getRowMetaData(), builder.build());
                                    synchronized (resMap) {
                                        List<RowObservable> rowObservables = resMap.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
                                        rowObservables.add(observable);
                                    }
                                    promise.complete();
                                });
                        return promise.future();
                    });
                });
            }
        }
        return future.flatMap(unused -> {
            SQLKey sqlKey = res.get(0);
            Future<RowObservable> rowObservableFuture = VertxExecuter.runQuery(
                    Future.succeededFuture(connection2),
                    sqlKey.getSql().getSql(),
                    MycatPreparedStatementUtil.extractParams(params, sqlKey.getSql().getDynamicParameters()));
            return rowObservableFuture.map(rowObservable -> {
                synchronized (ProxyConnectionUsage.this) {
                    List<RowObservable> rowObservables = resMap.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
                    rowObservables.add(rowObservable);
                }
                return null;
            });
        });
    }

    private Future<Map<String, LinkedList<SqlConnection>>> getConnection(XaSqlConnection connection) {

        List<String> strings = targets.stream().map(i ->
                context.resolveDatasourceTargetName(i.getTargetName())).collect(Collectors.toList());
        Map<String, LinkedList<SqlConnection>> map = new ConcurrentHashMap<>();

        PromiseInternal<Map<String, LinkedList<SqlConnection>>> promise = VertxUtil.newPromise();
        java.util.concurrent.Future<Future<?>> submit = getTrxExecutorService().submit(() -> {
            try {
                Future<Void> future = Future.succeededFuture();
                for (String string : strings) {
                    future = future.flatMap(unused -> {
                        Future<SqlConnection> connection1 = connection.getConnection(string);
                        return connection1.map(i -> {
                            synchronized (ProxyConnectionUsage.this) {
                                LinkedList<SqlConnection> defaultConnections = map.computeIfAbsent(string, s -> new LinkedList<>());
                                defaultConnections.add(i);
                            }
                            return null;
                        });
                    });
                }
                return future.onComplete(event -> {
                    if (event.succeeded()) {
                        promise.tryComplete(map);
                    } else {
                        promise.fail(event.cause());
                    }
                });
            } catch (Throwable throwable) {
                promise.fail(throwable);
            }
            return promise;
        });
        return promise.future();
    }

    private Future<Map<String, List<SQLKey>>> getConnectionWhenTranscation(XaSqlConnection connection) {
        Map<String, List<SQLKey>> map = targets.stream().collect(Collectors.groupingBy(i ->
                context.resolveDatasourceTargetName(i.getTargetName(), context.isInTransaction())));
        TransactionSession transactionSession = context.getTransactionSession();
        PromiseInternal<Map<String, List<SQLKey>>> promise = VertxUtil.newPromise();
        getTrxExecutorService().submit(() -> {
            try {
                Future<Void> future = Future.succeededFuture();
                for (String s : map.keySet()) {
                    future = future
                            .flatMap(unused ->
                                    (Future) connection.getConnection(transactionSession.resolveFinalTargetName(s, true)));
                }
                return future.onComplete(event -> {
                    if (event.succeeded()) {
                        promise.tryComplete(map);
                    } else {
                        promise.fail(event.cause());
                    }
                });
            } catch (Throwable throwable) {
                promise.fail(throwable);
            }
            return promise;
        });
        return promise;
    }
}
