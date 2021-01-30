package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.executeQuery;

public  class JdbcConnectionUsage {
    private final  MycatDataContext context;
    private final List<SQLKey> targets;
    public static JdbcConnectionUsage computeTargetConnection(MycatDataContext context, List<Object> params, CodeExecuterContext executerContext) {
        List<SQLKey> sqlKeys = new ArrayList<>();
        List<RelNode> mycatViews = executerContext.getMycatViews();
        for (RelNode mycatView : mycatViews) {
            if (mycatView instanceof MycatView) {
                MycatView mycatView1 = (MycatView) mycatView;
                ImmutableMultimap<String, SqlString> multimap = mycatView1.expandToSql(executerContext.isForUpdate(), params);
                for (Map.Entry<String, SqlString> entry : multimap.entries()) {
                    String targetName = entry.getKey();
                    SqlString value = entry.getValue();
                    sqlKeys.add(new SQLKey(
                            mycatView1,
                            targetName,
                            value));
                }
            } else if (mycatView instanceof MycatTransientSQLTableScan) {
                MycatTransientSQLTableScan transientSQLTableScan = (MycatTransientSQLTableScan) mycatView;
                sqlKeys.add(new SQLKey(
                        mycatView,
                        transientSQLTableScan.getTargetName(),
                        new SqlString(MycatSqlDialect.DEFAULT, transientSQLTableScan.getSql())));
            } else {
                throw new UnsupportedOperationException();
            }
        }
        return new JdbcConnectionUsage(context, sqlKeys);
    }

    public JdbcConnectionUsage(MycatDataContext context,
                               List<SQLKey> map) {
        this.context = context;
        this.targets = map;
    }

    public CompletableFuture<IdentityHashMap<RelNode, List<Enumerable<Object[]>>>> collect(JdbcConnectionManager connectionManager, List<Object> params) {
        Map<String, List<SQLKey>> map = Collections.emptyMap();
        Map<String, LinkedList<DefaultConnection>> list = Collections.emptyMap();
        if (context.isInTransaction()) {
            map = getConnectionWhenTranscation(connectionManager);
        } else {
            list = getConnection(connectionManager, context.getTransactionSession());
        }
        IdentityHashMap<RelNode, List<Enumerable<Object[]>>> res = new IdentityHashMap<>();
        Map<RelNode, List<Enumerable<Object[]>>> identityHashMap = Collections.synchronizedMap(res);
        TransactionSession transactionSession = context.getTransactionSession();
        List<CompletableFuture> all = new ArrayList<>();
        CompletableFuture<Void> voidCompletableFuture;
        if (context.isInTransaction()) {
            ArrayList<Map.Entry<String, List<SQLKey>>> entries = new ArrayList<>(map.entrySet());
            if (entries.size() > 1) {
                for (Map.Entry<String, List<SQLKey>> stringListEntry : entries.subList(1, entries.size())) {
                    DefaultConnection jdbcConnection = (DefaultConnection)
                            transactionSession
                                    .getJDBCConnection(context.resolveDatasourceTargetName(stringListEntry.getKey()));
                    all.add(CompletableFuture.runAsync(() -> {
                        collectResultSetOnOneConnection(params, identityHashMap, jdbcConnection, stringListEntry.getValue());
                    }));
                }
            }
            Map.Entry<String, List<SQLKey>> stringListEntry = entries.get(0);
            DefaultConnection jdbcConnection = (DefaultConnection)
                    transactionSession
                            .getJDBCConnection(context.resolveDatasourceTargetName(stringListEntry.getKey()));
            collectResultSetOnOneConnection(params, identityHashMap, jdbcConnection, stringListEntry.getValue());
            voidCompletableFuture = CompletableFuture.allOf(all.toArray(new CompletableFuture[]{}));
        } else {
            if (targets.size() > 1) {
                for (SQLKey target : targets.subList(1, targets.size())) {
                    LinkedList<DefaultConnection> defaultConnections = list.get(context.resolveDatasourceTargetName(target.getTargetName()));
                    DefaultConnection defaultConnection = defaultConnections.pop();
                    all.add(CompletableFuture.runAsync(() -> {
                        collectResultSet(params, identityHashMap, defaultConnection, false, target);
                    }));
                }
            }
            SQLKey sqlKey = targets.get(0);
            LinkedList<DefaultConnection> defaultConnections = list.get(context.resolveDatasourceTargetName(sqlKey.getTargetName()));
            DefaultConnection defaultConnection = defaultConnections.pop();
            collectResultSet(params, identityHashMap, defaultConnection, false, sqlKey);
            voidCompletableFuture = CompletableFuture.allOf(all.toArray(new CompletableFuture[]{}));
        }
        return voidCompletableFuture.thenCompose(unused -> CompletableFuture.completedFuture(res));
    }

    private static void collectResultSetOnOneConnection(List<Object> params,
                                                        Map<RelNode, List<Enumerable<Object[]>>> map,
                                                        DefaultConnection jdbcConnection,
                                                        List<SQLKey> value) {
        int size = value.size();
        if (size > 1) {
            boolean copy = true;
            for (SQLKey sqlKey : value.subList(1, size)) {
                collectResultSet(params, map, jdbcConnection, copy, sqlKey);
            }
        }
        collectResultSet(params, map, jdbcConnection, false, value.get(0));
    }

    private static void collectResultSet(List<Object> params,
                                         Map<RelNode, List<Enumerable<Object[]>>> map,
                                         DefaultConnection jdbcConnection,
                                         boolean copy,
                                         SQLKey sqlKey) {
        RowBaseIterator rowBaseIterator = executeQuery(jdbcConnection.unwrap(Connection.class),
                jdbcConnection, new CalciteRowMetaData(sqlKey.getMycatView().getRowType().getFieldList()),
                sqlKey.getSql(), params, null);
        Enumerable<Object[]> objects = toEnumerable(rowBaseIterator, copy);
        List<Enumerable<Object[]>> enumerables = map.computeIfAbsent(sqlKey.getMycatView(), node -> new ArrayList<>());
        synchronized (enumerables) {
            enumerables.add(objects);
        }
    }

    @NotNull
    private static Enumerable<Object[]> toEnumerable(RowBaseIterator rowBaseIterator, boolean copy) {
        MycatRowMetaData metaData = rowBaseIterator.getMetaData();
        int columnCount = metaData.getColumnCount();
        if (copy) {
            ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
            while (rowBaseIterator.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rowBaseIterator.getObject(i);
                }
                builder.add(row);
            }
            return Linq4j.asEnumerable(builder.build());
        } else {
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new Enumerator<Object[]>() {
                        @Override
                        public Object[] current() {
                            return rowBaseIterator.getObjects(columnCount);
                        }

                        @Override
                        public boolean moveNext() {
                            return rowBaseIterator.next();
                        }

                        @Override
                        public void reset() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void close() {
                            rowBaseIterator.close();
                        }
                    };
                }
            };
        }
    }

    private Map<String, LinkedList<DefaultConnection>> getConnection(JdbcConnectionManager connectionManager, TransactionSession transactionSession) {
        Map<String, LinkedList<DefaultConnection>> list;
        List<String> strings = targets.stream().map(i ->
                context.resolveDatasourceTargetName(i.getTargetName())).collect(Collectors.toList());
        list = new HashMap<>();
        synchronized (connectionManager) {
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            for (String s : strings) {
                DefaultConnection connection = jdbcConnectionManager.getConnection(s);
                transactionSession.addCloseResource(connection);
                LinkedList<DefaultConnection> defaultConnections = list.computeIfAbsent(s, s1 -> new LinkedList<>());
                defaultConnections.add(connection);
            }
        }
        return list;
    }

    private Map<String, List<SQLKey>> getConnectionWhenTranscation(JdbcConnectionManager connectionManager) {
        Map<String, List<SQLKey>> map = targets.stream().collect(Collectors.groupingBy(i ->
                context.resolveDatasourceTargetName(i.getTargetName(), context.isInTransaction())));
        TransactionSession transactionSession = context.getTransactionSession();
        synchronized (connectionManager) {
            for (Map.Entry<String, List<SQLKey>> e : map.entrySet()) {
                String key = e.getKey();
                transactionSession.getJDBCConnection(key);
            }
        }
        return map;
    }
}