package io.mycat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.api.collector.ComposeFutureRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorCloseCallback;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlrecorder.SqlRecord;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable2;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CodeExecuterContext;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Future;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.executeQuery;
import static io.mycat.calcite.executor.MycatPreparedStatementUtil.outputToParameterized;

public class NewMycatDataContextImpl implements NewMycatDataContext {
    private final MycatDataContext dataContext;
    private final CodeExecuterContext context;
    private final List<Object> params;
    private final boolean forUpdate;
    private final IdentityHashMap<Object, Queue<Enumerable<Object[]>>> identityHashMap = new IdentityHashMap<>();
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NewMycatDataContextImpl.class);

    public NewMycatDataContextImpl(MycatDataContext dataContext,
                                   CodeExecuterContext context,
                                   List<Object> params,
                                   boolean forUpdate) {
        this.dataContext = dataContext;
        this.context = context;
        this.params = params;
        this.forUpdate = forUpdate;
    }

    @Override
    public SchemaPlus getRootSchema() {
        return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        if (name.startsWith("?")){
            int index = Integer.parseInt(name.substring(1));
            return params.get(index);
        }
        return context.getContext().get(name);
    }

    @SneakyThrows
    public void allocateResource() {
        TransactionSession transactionSession = dataContext.getTransactionSession();
        for (RelNode relNode : context.getMycatViews()) {
            if (relNode instanceof MycatView) {
                MycatView mycatView = (MycatView) relNode;
                if (transactionSession.isInTransaction()){
                    onHeap(mycatView);
                }else{
                    onParellel(mycatView);
                }
            }else if (relNode instanceof MycatTransientSQLTableScan) {
                Queue<Enumerable<Object[]>> list = getEnumerableList(relNode);
                list.add(Linq4j.asEnumerable(new AbstractEnumerable2<Object[]>() {
                    @NotNull
                    @Override
                    public Iterator<Object[]> iterator() {
                        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                        MycatTransientSQLTableScan transientSQLTableScan = (MycatTransientSQLTableScan) relNode;
                        String targetName = transientSQLTableScan.getTargetName();
                        String sql = transientSQLTableScan.getSql();
                        DefaultConnection mycatConnection = jdbcConnectionManager.getConnection(targetName);
                        dataContext.getTransactionSession().addCloseResource(mycatConnection);
                        RowBaseIterator rowBaseIterator = mycatConnection.executeQuery(sql);
                        int columnCount = rowBaseIterator.getMetaData().getColumnCount();
                        return new Iterator<Object[]>() {

                            @Override
                            public boolean hasNext() {
                                return rowBaseIterator.next();
                            }

                            @Override
                            public Object[] next() {
                                Object[] row = new Object[columnCount];
                                for (int i = 0; i < columnCount; i++) {
                                    row[i] = rowBaseIterator.getObject(i );
                                }
                                return row;
                            }
                        };
                    }
                }));
            }
        }
    }

    @SneakyThrows
    private void onParellel(MycatView mycatView) {
        Queue<Enumerable<Object[]>> list = getEnumerableList(mycatView);
        list.add(Linq4j.asEnumerable(new AbstractEnumerable2<Object[]>() {
            @NotNull
            @SneakyThrows
            @Override
            public Iterator<Object[]> iterator() {
                MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                NameableExecutor mycatWorker = mycatWorkerProcessor.getMycatWorker();
                LinkedList<Future<RowBaseIterator>> futureArrayList = new LinkedList<>();
                TransactionSession transactionSession = dataContext.getTransactionSession();
                SqlRecord sqlRecord = dataContext.currentSqlRecord();
                JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(mycatView.getRowType().getFieldList());
                ImmutableMultimap<String, SqlString> expandToSqls = mycatView.expandToSql(forUpdate, params);
                synchronized (jdbcConnectionManager) {
                    for (Map.Entry<String, SqlString> entry : expandToSqls.entries()) {
                        String target = entry.getKey();
                        MycatConnection mycatConnection = jdbcConnectionManager.getConnection(target);
                        transactionSession.addCloseResource(mycatConnection);
                        Connection connection = mycatConnection.unwrap(Connection.class);
                        if (connection.isClosed()) {
                            LOGGER.error("mycatConnection:{} has closed but still using", mycatConnection);
                        }
                        long start = SqlRecord.now();
                        SqlString sqlString = entry.getValue();
                        futureArrayList.add(
                                mycatWorker.submit(() -> {
                                    return executeQuery(connection, mycatConnection, calciteRowMetaData, sqlString, params,
                                            rowCount -> sqlRecord.addSubRecord(sqlString, start, SqlRecord.now(), target, rowCount)
                                    );
                                })
                        );
                    }
                }

                ComposeFutureRowBaseIterator composeFutureRowBaseIterator = new ComposeFutureRowBaseIterator(calciteRowMetaData, futureArrayList);
                int columnCount = calciteRowMetaData.getColumnCount();
                return new Iterator<Object[]>() {
                    @Override
                    public boolean hasNext() {
                        return composeFutureRowBaseIterator.next();
                    }

                    @Override
                    public Object[] next() {
                        Object[] row = new Object[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            row[i] = composeFutureRowBaseIterator.getObject(i);
                        }
                        return row;
                    }
                };
            }
        }));
    }

    private Queue<Enumerable<Object[]>> getEnumerableList(RelNode mycatView) {
        Queue<Enumerable<Object[]>> list;
        if(!identityHashMap.containsKey(mycatView)){
            identityHashMap.put(mycatView,list =new LinkedList<>());
        }else {
            list = identityHashMap.get(mycatView);
        }
        return list;
    }

    private void onHeap(MycatView mycatView) {
        SqlRecord sqlRecord = dataContext.currentSqlRecord();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(mycatView.getRowType().getFieldList());
        ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
        for (Map.Entry<String, SqlString> entry : mycatView.expandToSql(forUpdate, params).entries()) {
            String target = transactionSession.resolveFinalTargetName(entry.getKey());
            MycatConnection connection = transactionSession.getConnection(target);
            long start = SqlRecord.now();
            SqlString sqlString = entry.getValue();
            try (RowBaseIterator rowIterator = executeQuery(connection.unwrap(Connection.class), connection, calciteRowMetaData, sqlString, params, new RowIteratorCloseCallback() {
                @Override
                public void onClose(long rowCount) {
                    sqlRecord.addSubRecord(sqlString, start, SqlRecord.now(), target, rowCount);
                }
            })) {
                MycatRowMetaData metaData = rowIterator.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rowIterator.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = rowIterator.getObject(i);
                    }
                    builder.add(row);
                }
            }
        }
        ImmutableList<Object[]> objects = builder.build();
        Queue<Enumerable<Object[]>> list = getEnumerableList(mycatView);
        list.add( Linq4j.asEnumerable(new AbstractEnumerable2<Object[]>() {
            @NotNull
            @Override
            public Iterator<Object[]> iterator() {
                return objects.iterator();
            }
        }));
    }

    @Override
    public Enumerable<Object[]> getEnumerable(RelNode node) {
        Queue<Enumerable<Object[]>> enumerables = identityHashMap.get(node);
      return   enumerables.remove();
    }


    public Object getSessionVariable(String name) {
        return dataContext.getVariable(false, name);
    }

    public Object getGlobalVariable(String name) {
        return dataContext.getVariable(true, name);
    }

    public String getDatabase() {
        return dataContext.getDefaultSchema();
    }

    public Long getLastInsertId() {
        return dataContext.getLastInsertId();
    }

    public Long getConnectionId() {
        return dataContext.getSessionId();
    }

    public Object getUserVariable(String name) {
        return null;
    }

    public String getCurrentUser() {
        MycatUser user = dataContext.getUser();
        Authenticator authenticator = MetaClusterCurrent.wrapper(Authenticator.class);
        return user.getUserName() + "@" + authenticator.getUserInfo(user.getUserName()).getIp();
    }

    public String getUser() {
        MycatUser user = dataContext.getUser();
        return user.getUserName() + "@" + user.getHost();
    }
}
