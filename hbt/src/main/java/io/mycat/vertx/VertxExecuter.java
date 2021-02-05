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
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VertxExecuter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxExecuter.class);

    public static Future<long[]> runMycatInsertRel(XaSqlConnection sqlConnection,
                                                   MycatDataContext context,
                                                   MycatInsertRel insertRel,
                                                   List<Object> params) {
        MycatInsertExecutor insertExecutor = MycatInsertExecutor.create(context, insertRel, params);
        Map<SQL, Group> groupMap = insertExecutor.getGroupMap();
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
            list.add(runInsert(insertMap, sqlConnection.getConnection(transactionSession.resolveFinalTargetName(entry.getKey()))));
        }
        if (list.isEmpty()) {
            throw new AssertException();
        }
        return CompositeFuture.all((List) list).map(new SumInsertResult(list));

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
            res.add(runUpdate(e.getValue().stream().collect(Collectors.toMap(SQL::getParameterizedSql, SQL::getParameters,
                    (a, b) -> b)),
                    sqlConnection.getConnection(targetMap.get(e.getKey()))));
        }
        return CompositeFuture.all((List) res).map(new SumUpdateResult(updateRel, res));
    }

    public static RowObservable runQuery(Future<SqlConnection> sqlConnectionFuture, String sql, List<Object> values) {
        return new RowObservableImpl(sqlConnectionFuture, sql, values);
    }

    static class RowObservableImpl extends RowObservable implements StreamMysqlCollector {
        MycatRowMetaData metaData;
        private Observer<? super Object[]> observer;
        private final Future<SqlConnection> sqlConnectionFuture;
        private final String sql;
        private final List<Object> values;
        private ColumnDefinition[] columnDefinitions;

        public RowObservableImpl(Future<SqlConnection> sqlConnectionFuture, String sql, List<Object> values) {
            this.sqlConnectionFuture = sqlConnectionFuture;
            this.sql = sql;
            this.values = values;


        }

        @Override
        public MycatRowMetaData getRowMetaData() {
            return metaData;
        }

        @Override
        protected void subscribeActual(@NonNull Observer<? super Object[]> observer) {
            this.observer = observer;
            sqlConnectionFuture
                    .flatMap(connection -> connection.prepare(sql)).compose(preparedStatement -> {
                PreparedQuery<RowSet<Row>> query = preparedStatement.query();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("RowObservableImpl sql:{} connection:{}", sql, sqlConnectionFuture.result());
                }
                PreparedQuery<SqlResult<Void>> collecting = query.collecting(this);
                return collecting.execute(Tuple.tuple(values));
            }).onSuccess(new Handler<SqlResult<Void>>() {
                @Override
                public void handle(SqlResult<Void> event) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("subscribeActual successful sql:{} connection:{}", sql, sqlConnectionFuture.result());
                    }
                    observer.onComplete();
                }
            }).onFailure(event -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.error("subscribeActual error sql:{}", sql);
                }
                this.observer.onError(event);
            });
        }

        @Override
        public void onColumnDefinitions(MySQLRowDesc columnDefinitions, QueryCommandBase queryCommand) {
            this.columnDefinitions = columnDefinitions.columnDefinitions();
            this.metaData = toColumnMetaData(columnDefinitions.columnDescriptor());
            this.observer.onSubscribe(Disposable.disposed());
        }

        @Override
        public void onRow(Row row) {
            int size = this.columnDefinitions.length;
            Object[] objects = new Object[size];
            for (int i = 0; i < size; i++) {
                switch (this.columnDefinitions[i].type()) {
                    case INT1:
                    case INT2:
                    case INT3:
                    case INT4:
                        objects[i] = row.getLong(i);
                        break;
                    case NUMERIC:
                    case INT8:
                        objects[i] = row.getBigDecimal(i);
                        break;
                    case DOUBLE:
                    case FLOAT:
                        objects[i] = row.getDouble(i);
                        break;
                    case STRING:
                    case VARSTRING:
                        objects[i] = row.getString(i);
                        break;
                    case LONGBLOB:
                    case MEDIUMBLOB:
                    case BLOB:
                    case TINYBLOB:
                        Buffer buffer = row.getBuffer(i);
                        if (buffer == null) {
                            objects[i] = null;
                        } else {
                            objects[i] = buffer.getBytes();
                        }
                        break;
                    case DATE:
                        objects[i] = row.getLocalDate(i);
                        break;
                    case TIME:
                        objects[i] = (Duration) row.getValue(i);
                        break;
                    case DATETIME:
                        objects[i] = row.getLocalDateTime(i);
                        break;
                    case YEAR:
                        objects[i] = row.getShort(i);
                        break;
                    case TIMESTAMP:
                        objects[i] = row.getLocalDateTime(i);
                        break;
                    case BIT:
                        objects[i] = row.getBoolean(i);
                        break;
                    case JSON:
                        objects[i] = row.getString(i);
                        break;
                    case NULL:
                        objects[i] = null;
                        break;
                    case UNBIND:
                    case GEOMETRY:
                    default:
                        throw new IllegalStateException("Unexpected value: " + this.columnDefinitions[i].type());
                }
            }
            observer.onNext(objects);
        }
    }

    public static RowObservable runQuery(Future<SqlConnection> sqlConnectionFuture,
                                         String sql) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("runQuery");
        }
        return new RowObservableImpl(sqlConnectionFuture, sql, Collections.emptyList());
    }

    private static MycatRowMetaData toColumnMetaData(List<ColumnDescriptor> event) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        for (ColumnDescriptor columnDescriptor : event) {
            resultSetBuilder.addColumnInfo(columnDescriptor.name(),
                    columnDescriptor.jdbcType());
        }
        RowBaseIterator build = resultSetBuilder.build();
        return build.getMetaData();
    }

    public static Future<long[]> runUpdate(Future<SqlConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.query(sql).execute().map(r -> new long[]{r.rowCount(), Optional.ofNullable(r.property(MySQLClient.LAST_INSERTED_ID)).orElse(0L)}));
    }

    public static Future<long[]> runUpdate(Map<String, List<Object>> updateMap,
                                           Future<SqlConnection> sqlConnectionFuture) {
        List<long[]> list = Collections.synchronizedList(new ArrayList<>());
        Future future = Future.succeededFuture();
        for (Map.Entry<String, List<Object>> e : updateMap.entrySet()) {
            String sql = e.getKey();
            List<Object> values = e.getValue();
            future = future.flatMap(new UpdateByConnection(sqlConnectionFuture, sql, values, list));
        }
        return future.map(new SimpleSumUpdateResult(list));
    }

    public static Future<long[]> runInsert(
            Map<String, List<List<Object>>> insertMap, Future<SqlConnection> sqlConnectionFuture) {
        List<long[]> list = Collections.synchronizedList(new ArrayList<>());
        Future<Void> future = Future.succeededFuture();
        for (Map.Entry<String, List<List<Object>>> e : insertMap.entrySet()) {
            String sql = e.getKey();
            List<List<Object>> values = e.getValue();
            future = future.flatMap(unused -> {
                return sqlConnectionFuture
                        .flatMap(new Function<SqlConnection, Future<Void>>() {
                            @Override
                            public Future<Void> apply(SqlConnection connection) {
                                Future<Void> future2 = connection.prepare(sql).flatMap(preparedStatement -> {
                                    List<Tuple> collect = values.stream().map(u -> Tuple.from(u)).collect(Collectors.toList());
                                    Future<RowSet<Row>> rowSetFuture = preparedStatement.query().executeBatch(collect);
                                    Future<Void> map = rowSetFuture.map(rows -> {
                                        int affectedRow = rows.rowCount();
                                        long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                                                .orElse(0L);
                                        list.add(new long[]{affectedRow, lastInsertId});
                                        return null;
                                    });
                                    return map;
                                });
                                return future2;
                            }
                        });
            });
        }
        return future.map(new SimpleSumInsertResult(list));
    }

    private static class SumInsertResult implements Function<CompositeFuture, long[]> {
        private final List<Future<long[]>> list;

        public SumInsertResult(List<Future<long[]>> list) {
            this.list = list;
        }

        @Override
        public long[] apply(CompositeFuture r) {
            return list.stream().map(l -> l.result())
                    .reduce((longs, longs2) ->
                            new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                    .orElse(new long[2]);
        }
    }

    private static class SumUpdateResult implements Function<CompositeFuture, long[]> {
        private final MycatUpdateRel updateRel;
        private final List<Future<long[]>> res;

        public SumUpdateResult(MycatUpdateRel updateRel, List<Future<long[]>> res) {
            this.updateRel = updateRel;
            this.res = res;
        }

        @Override
        public long[] apply(CompositeFuture r) {
            return updateRel.isGlobal() ? res.get(0).result() :
                    res.stream().map(l -> l.result())
                            .reduce((longs, longs2) ->
                                    new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                            .orElse(new long[2]);
        }
    }

    private static class SimpleSumUpdateResult implements Function<long[], long[]> {
        private final List<long[]> list;

        public SimpleSumUpdateResult(List<long[]> list) {
            this.list = list;
        }

        @Override
        public long[] apply(long[] longs) {
            return list.stream()
                    .reduce(new long[]{0, 0},
                            (longs1, longs2) ->
                                    new long[]{longs1[0] + longs2[0],
                                            Math.max(longs1[1], longs2[1])});
        }
    }

    private static class SimpleSumInsertResult implements Function<Void, long[]> {
        private final List<long[]> list;

        public SimpleSumInsertResult(List<long[]> list) {
            this.list = list;
        }

        @Override
        public long[] apply(Void unused) {
            return list.stream()
                    .reduce(new long[]{0, 0},
                            (longs1, longs2) ->
                                    new long[]{longs1[0] + longs2[0],
                                            Math.max(longs1[1], longs2[1])});
        }
    }

    private static class UpdateByConnection implements Function {
        private final Future<SqlConnection> sqlConnectionFuture;
        private final String sql;
        private final List<Object> values;
        private final List<long[]> list;

        public UpdateByConnection(Future<SqlConnection> sqlConnectionFuture, String sql, List<Object> values, List<long[]> list) {
            this.sqlConnectionFuture = sqlConnectionFuture;
            this.sql = sql;
            this.values = values;
            this.list = list;
        }

        @Override
        public Object apply(Object unused) {
            return sqlConnectionFuture
                    .flatMap(connection -> connection.prepare(sql)
                            .flatMap(preparedStatement -> {
                                Future<RowSet<Row>> rowSetFuture;
                                if (!values.isEmpty() && values.get(0) instanceof List) {
                                    rowSetFuture = preparedStatement.query().executeBatch(values.stream().map(i -> Tuple.from((List<Object>) i)).collect(Collectors.toList()));
                                } else {
                                    rowSetFuture = preparedStatement.query().execute(Tuple.from(values));
                                }
                                return rowSetFuture.map(new UpdateResultCollector());
                            }));
        }

        private class UpdateResultCollector implements Function<RowSet<Row>, Object> {
            @Override
            public Object apply(RowSet<Row> rows) {
                int affectedRow = rows.rowCount();
                long lastInsertId = Optional.ofNullable(rows.property(MySQLClient.LAST_INSERTED_ID))
                        .orElse(0L);
                list.add(new long[]{affectedRow, lastInsertId});
                return null;
            }
        }
    }
}
