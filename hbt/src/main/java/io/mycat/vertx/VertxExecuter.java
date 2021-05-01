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
package io.mycat.vertx;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.mchange.util.AssertException;
import io.mycat.MycatDataContext;
import io.mycat.Process;
import io.mycat.TransactionSession;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.executor.Group;
import io.mycat.calcite.executor.MycatInsertExecutor;
import io.mycat.calcite.executor.MycatUpdateExecutor;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.util.SQL;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return CompositeFuture.all((List) list)
                .map(compositeFuture -> list.stream().map(l -> l.result())
                        .reduce((longs, longs2) ->
                                new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                        .orElse(new long[2])).onComplete(event -> sqlConnection.closeStatementState());
    }


    public static Future<long[]> runMycatUpdateRel(XaSqlConnection sqlConnection, MycatDataContext context, MycatUpdateRel updateRel, List<Object> params) {
        final Set<SQL> reallySqlSet = MycatUpdateExecutor.buildReallySqlList(updateRel,
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
        return CompositeFuture.all((List) res).map(new SumUpdateResult(updateRel.isGlobal(), res))
                .onComplete(new Handler<AsyncResult<long[]>>() {
                    @Override
                    public void handle(AsyncResult<long[]> event) {
                        sqlConnection.closeStatementState();
                    }
                });
    }

    public static Observable<MysqlPayloadObject> runQueryOutputAsMysqlPayloadObject(Future<SqlConnection> connectionFuture,
                                                                                    String sql,
                                                                                    List<Object> values) {
        return Observable.create(emitter -> {
            // 连接到达
            connectionFuture.onSuccess(connection -> {
                // 预编译到达
                Process.getCurrentProcess().trace(connection).prepare(sql)
                        .onSuccess(preparedStatement -> {
                            // 查询结果到达
                            PreparedQuery<RowSet<Row>> query = preparedStatement.query();
                            query.collecting(new EmitterMysqlPayloadCollector(emitter, null, true)).execute(Tuple.tuple(values))
                                    .onSuccess(event -> emitter.onComplete())
                                    .onFailure(throwable -> emitter.onError(throwable));
                        })
                        .onFailure(throwable -> {
                            emitter.onError(throwable);
                        });
            }).onFailure(throwable -> {
                emitter.onError(throwable);
            });
        });

    }

    public static Observable<Object[]> runQuery(Future<SqlConnection> connectionFuture,
                                                String sql,
                                                List<Object> values,
                                                MycatRowMetaData rowMetaData) {
        return Observable.create(emitter -> {
            // 连接到达
            connectionFuture.onSuccess(connection -> {
                // 预编译到达
                Process.getCurrentProcess().trace(connection).prepare(sql)
                        .onSuccess(preparedStatement -> {
                            // 查询结果到达
                            PreparedQuery<RowSet<Row>> query = preparedStatement.query();
                            query.collecting(new EmitterObjectsCollector(emitter, rowMetaData)).execute(Tuple.tuple(values))
                                    .onSuccess(event -> emitter.onComplete())
                                    .onFailure(throwable -> emitter.onError(throwable));
                        })
                        .onFailure(throwable -> {
                            emitter.onError(throwable);
                        });
            }).onFailure(throwable -> {
                emitter.onError(throwable);
            });
        });
    }

    public static Future<long[]> runUpdate(Future<SqlConnection> sqlConnectionFuture, String sql) {
        return sqlConnectionFuture.flatMap(c -> c.query(sql).execute()
                .map(r -> new long[]{r.rowCount(), Optional.ofNullable(r.property(MySQLClient.LAST_INSERTED_ID)).orElse(0L)}));
    }

    public static Future<long[]> runUpdate(Map<String, List<Object>> updateMap,
                                           Future<SqlConnection> sqlConnectionFuture) {
        List<long[]> list = Collections.synchronizedList(new ArrayList<>());
        Future<Void> future = Future.succeededFuture();
        for (Map.Entry<String, List<Object>> e : updateMap.entrySet()) {
            String sql = e.getKey();
            List<Object> values = e.getValue();
            future = future.flatMap(new UpdateByConnection(sqlConnectionFuture, sql, values, list));
        }
        return future.map(sumUpdateResult(list));
    }

    public static Future<long[]> runInsert(
            Map<String, List<List<Object>>> insertMap,
            Future<SqlConnection> sqlConnectionFuture) {
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
                                Future<Void> future2 = Process.getCurrentProcess().trace(connection).prepare(sql).flatMap(preparedStatement -> {
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
        return future.map(sumUpdateResult(list));
    }

    @NotNull
    private static Function<Void, long[]> sumUpdateResult(List<long[]> list) {
        return unused -> list.stream()
                .reduce(new long[]{0, 0},
                        (longs1, longs2) ->
                                new long[]{longs1[0] + longs2[0],
                                        Math.max(longs1[1], longs2[1])});
    }

    private static class SumUpdateResult implements Function<CompositeFuture, long[]> {
        private final boolean global;
        private final List<Future<long[]>> res;

        public SumUpdateResult(boolean global, List<Future<long[]>> res) {
            this.global = global;
            this.res = res;
        }

        @Override
        public long[] apply(CompositeFuture r) {
            return this.global ? res.get(0).result() :
                    res.stream().map(l -> l.result())
                            .reduce((longs, longs2) ->
                                    new long[]{longs[0] + longs2[0], Math.max(longs[1], longs2[1])})
                            .orElse(new long[2]);
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
                    .flatMap(connection -> Process.getCurrentProcess().trace(connection).prepare(sql)
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
