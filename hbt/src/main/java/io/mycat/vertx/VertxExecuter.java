package io.mycat.vertx;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.mchange.util.AssertException;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.executor.Group;
import io.mycat.calcite.executor.MycatInsertExecutor;
import io.mycat.calcite.executor.MycatUpdateExecutor;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.util.SQL;
import io.mycat.util.VertxUtil;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.subscriptions.AsyncSubscription;
import io.reactivex.rxjava3.processors.AsyncProcessor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class VertxExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxExecuter.class);

    public static Future<long[]> runMycatInsertRel(XaSqlConnection sqlConnection,
                                                   MycatDataContext context,
                                                   MycatInsertRel insertRel,
                                                   List<Object> params) {
        MycatInsertExecutor insertExecutor = MycatInsertExecutor.create(context, insertRel, params);
        Map<SQL, Group> groupMap = insertExecutor.getGroupMap();
        HashMap<String, List<List<Object>>> map = new HashMap<>();
        Map<String, List<Map.Entry<SQL, Group>>> map1 = groupMap.entrySet().stream().collect(Collectors.groupingBy(i -> i.getKey().getTarget()));
        TransactionSession transactionSession = context.getTransactionSession();
        List<Future<long[]>> list = new LinkedList<>();
        for (Map.Entry<String, List<Map.Entry<SQL, Group>>> entry : map1.entrySet()) {
            List<Map.Entry<SQL, Group>> value = entry.getValue();

            HashMap<String, List<List<Object>>> insertMap = new HashMap<>();
            for (Map.Entry<SQL, Group> e : value) {
                String parameterizedSql = e.getKey().getParameterizedSql();
                List<List<Object>> lists = insertMap.computeIfAbsent(parameterizedSql, s -> new LinkedList<>());
                lists.addAll(e.getValue().getArgs());
            }
            list.addAll(runInsert(insertMap, sqlConnection.getConnection(transactionSession.resolveFinalTargetName(entry.getKey()))));
        }
        if (list.isEmpty()) {
            throw new AssertException();
        }
        return CompositeFuture.all((List) list)
                .map(r -> {
                    return list.stream().map(l -> l.result())
                            .reduce((longs, longs2) ->
                                    new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                            .orElse(new long[2]);
                });

    }


    public static Future<long[]> runMycatUpdateRel(XaSqlConnection sqlConnection, MycatDataContext context, MycatUpdateRel updateRel, List<Object> params) {
        final Set<SQL> reallySqlSet = MycatUpdateExecutor.buildReallySqlList(updateRel, updateRel.getValues(),
                updateRel.getSqlStatement(),
                params);
        TransactionSession transactionSession = context.getTransactionSession();
        Map<String, String> targetMap = new HashMap<>();
        Set<String> uniqueValues = new HashSet<>();
        for (SQL sql : reallySqlSet) {
            String k = context.resolveDatasourceTargetName(sql.getTarget());
            if (uniqueValues.add(k)) {
                if (targetMap.put(sql.getTarget(), transactionSession.resolveFinalTargetName(k)) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }
        }
        Map<String, List<SQL>> targets = reallySqlSet.stream().collect(Collectors.groupingBy(k -> k.getTarget()));
        List<Future<long[]>> res = new LinkedList<>();
        for (Map.Entry<String, List<SQL>> e : targets.entrySet()) {
            res.addAll(runUpdate(e.getValue().stream().collect(Collectors.toMap(SQL::getParameterizedSql, SQL::getParameters,
                    (a, b) -> b)),
                    sqlConnection.getConnection(targetMap.get(e.getKey()))));
        }
        return CompositeFuture.all((List) res)
                .map(r -> {
                    return updateRel.isGlobal() ? res.get(0).result() :
                            res.stream().map(l -> l.result())
                                    .reduce((longs, longs2) ->
                                            new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                                    .orElse(new long[2]);
                });
    }


    public static CompositeFuture runMutliQuery(Future<SqlConnection> sqlConnectionFuture, List<String> sqls, List<Object> values) {
        ArrayList<Future> resList = new ArrayList<>();
        for (String sql : sqls) {
            Future<RowObservable> observableFuture = runQuery(sqlConnectionFuture, sql, values);
            resList.add(observableFuture);
        }
        return CompositeFuture.all(resList);
    }

    public static Future<RowObservable> runQuery(Future<SqlConnection> sqlConnectionFuture, String sql, List<Object> values) {
        PromiseInternal<RowObservable> promise = VertxUtil.newPromise();
        Future compose = sqlConnectionFuture
                .flatMap(connection -> connection.prepare(sql)).compose(preparedStatement -> {
                    PreparedQuery<RowSet<Row>> query = preparedStatement.query();
                    PreparedQuery<SqlResult<Void>> collecting = query.collecting(new C(promise));
                    return collecting.execute(Tuple.tuple(values));
                }).onFailure(event -> promise.tryFail(event));
        return promise;
    }
    static class C extends RowObservable implements StreamMysqlCollector{
        MycatRowMetaData metaData;
        private Observer<? super Object[]> observer;
        private PromiseInternal<RowObservable> promiseInternal;

        public C(PromiseInternal<RowObservable> promiseInternal) {
            this.promiseInternal = promiseInternal;
        }

        @Override
        public MycatRowMetaData getRowMetaData() {
            return metaData;
        }

        @Override
        protected void subscribeActual(@NonNull Observer<? super Object[]> observer) {
            this.observer = observer;
            this.observer.onSubscribe(Disposable.disposed());
        }

        @Override
        public void onColumnDefinitions(MySQLRowDesc columnDefinitions, QueryCommandBase queryCommand) {
            this.metaData = extracted(columnDefinitions.columnDescriptor());
            promiseInternal.tryComplete(this);
        }

        @Override
        public void onRow(Row row) {
            int size = row.size();
            Object[] objects = new Object[size];
            for (int i = 0; i < size; i++) {
                objects[i] = row.getValue(i);
            }
            observer.onNext(objects);
        }

        @Override
        public void onFinish(int sequenceId, int serverStatusFlags, long affectedRows, long lastInsertId) {
            this.observer.onComplete();
        }

        @Override
        public void close() throws IOException {

        }
    }

    public static Future<RowObservable> runQuery(Future<SqlConnection> sqlConnectionFuture,
                                                 String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("runQuery");
        }
        PromiseInternal<RowObservable> promise = VertxUtil.newPromise();
        sqlConnectionFuture
                .flatMap(connection -> {
                    Query<RowSet<Row>> query = connection.query(sql);
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("subscribeActual");
                    }
                    return query.collecting(new C(promise)).execute();
                }).onFailure(event -> promise.fail(event));
        return promise;
    }

    private static MycatRowMetaData extracted(List<ColumnDescriptor> event) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        for (ColumnDescriptor columnDescriptor : event) {
            resultSetBuilder.addColumnInfo(columnDescriptor.name(),
                    columnDescriptor.jdbcType());
        }
        RowBaseIterator build = resultSetBuilder.build();
        return build.getMetaData();
    }

    private Collector<Row, Observer, Observer> getCollector(ProcessMonitor monitor, Observer<Object[]> observer) {
        return new Collector<Row, Observer, Observer>() {
            @Override
            public Supplier<Observer> supplier() {
                return () -> observer;
            }

            @Override
            public BiConsumer<Observer, Row> accumulator() {
                return (aVoid, row) -> aVoid.onNext(row);
            }

            @Override
            public BinaryOperator<Observer> combiner() {
                return (aVoid, aVoid2) -> null;
            }

            @Override
            public Function<Observer, Observer> finisher() {
                return aVoid -> {
                    monitor.onFinish();
                    return null;
                };
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Collections.emptySet();
            }
        };
    }

    public static Future<long[]> runUpdate(Future<SqlConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.query(sql).execute().map(r -> new long[]{r.rowCount(), Optional.ofNullable(r.property(MySQLClient.LAST_INSERTED_ID)).orElse(0L)}));
    }

    public static List<Future<long[]>> runUpdate(Map<String, List<Object>> updateMap, Future<SqlConnection> sqlConnectionFuture) {
        List<Future<long[]>> list = new ArrayList<>();
        for (Map.Entry<String, List<Object>> e : updateMap.entrySet()) {
            String sql = e.getKey();
            List<Object> values = e.getValue();
            Future<long[]> future = sqlConnectionFuture.flatMap(connection -> connection.prepare(sql).flatMap(preparedStatement -> {
                Future<RowSet<Row>> rowSetFuture;
                if (!values.isEmpty() && values.get(0) instanceof List) {
                    rowSetFuture = preparedStatement.query().executeBatch(values.stream().map(i -> Tuple.from((List<Object>) i)).collect(Collectors.toList()));
                } else {
                    rowSetFuture = preparedStatement.query().execute(Tuple.from(values));
                }
                return rowSetFuture.map(rows -> {
                    int affectedRow = rows.rowCount();
                    long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                            .orElse(0L);
                    return (new long[]{affectedRow, lastInsertId});
                });
            }));
            list.add(future);
        }
        return list;
    }

    public static List<Future<long[]>> runInsert(
            Map<String, List<List<Object>>> insertMap, Future<SqlConnection> sqlConnectionFuture) {
        List<Future<long[]>> list = new ArrayList<>();
        for (Map.Entry<String, List<List<Object>>> e : insertMap.entrySet()) {
            String sql = e.getKey();
            List<List<Object>> values = e.getValue();
            Future<long[]> future = sqlConnectionFuture.flatMap(connection -> {
                return connection.prepare(sql).flatMap(preparedStatement -> {
                    List<Tuple> collect = values.stream().map(new Function<List<Object>, Tuple>() {
                        @Override
                        public Tuple apply(List<Object> u) {
                            return Tuple.from(u);
                        }
                    }).collect(Collectors.toList());
                    Future<RowSet<Row>> rowSetFuture = preparedStatement.query().executeBatch(collect);
                    return rowSetFuture.map(new Function<RowSet<Row>, long[]>() {
                        @Override
                        public long[] apply(RowSet<Row> rows) {
                            int affectedRow = rows.rowCount();
                            long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                                    .orElse(0L);
                            return (new long[]{affectedRow, lastInsertId});
                        }
                    });
                });
            });
            list.add(future);
        }
        return list;
    }

private interface ProcessMonitor {
    public void onStart();

    public void onRow(Row row);

    public void onFinish();

    public void onThrowable(Throwable throwable);

    public static Collector<Row, Void, ProcessMonitor> getCollector(ProcessMonitor monitor) {
        return new Collector<Row, Void, ProcessMonitor>() {
            @Override
            public Supplier<Void> supplier() {
                return () -> {
                    monitor.onStart();
                    return null;
                };
            }

            @Override
            public BiConsumer<Void, Row> accumulator() {
                return (aVoid, row) -> monitor.onRow(row);
            }

            @Override
            public BinaryOperator<Void> combiner() {
                return (aVoid, aVoid2) -> null;
            }

            @Override
            public Function<Void, ProcessMonitor> finisher() {
                return aVoid -> {
                    monitor.onFinish();
                    return monitor;
                };
            }

            @Override
            public Set<Characteristics> characteristics() {
                return Collections.emptySet();
            }
        };
    }
}
}
