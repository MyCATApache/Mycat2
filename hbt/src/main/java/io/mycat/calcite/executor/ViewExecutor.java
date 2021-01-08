package io.mycat.calcite.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.*;
import io.mycat.api.collector.ComposeFutureRowBaseIterator;
import io.mycat.api.collector.ComposeRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorCloseCallback;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.DataSourceFactory;
import io.mycat.calcite.Executor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.mpp.Row;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.util.Pair;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static io.mycat.calcite.executor.MycatPreparedStatementUtil.apply;
import static io.mycat.calcite.executor.MycatPreparedStatementUtil.executeQuery;

public class ViewExecutor implements Executor {
    private final boolean inTransaction;
    private MycatDataContext context;
    final MycatView view;
    private List<Object> params;
    final DataSourceFactory factory;
    private final ImmutableMultimap<String, SqlString> expandToSql;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ViewExecutor.class);


    public static ViewExecutor create(MycatDataContext context, MycatView view, boolean forUpdate, List<Object> params, DataSourceFactory factory) {
        return new ViewExecutor(context, view, forUpdate, params, factory);
    }

    protected ViewExecutor(MycatDataContext context, MycatView view, boolean forUpdate, List<Object> params, DataSourceFactory factory) {
        this.context = context;
        this.view = view;
        this.params = params;
        this.factory = factory;
        this.expandToSql = this.view.expandToSql(forUpdate, params);

        this.inTransaction = context.isInTransaction();
        List<String> targets = this.expandToSql.keys().asList();
        if (inTransaction) {
            factory.registered(new HashSet<>(targets));
        } else {
            factory.registered(targets);
        }

    }

    private MyCatResultSetEnumerator myCatResultSetEnumerator;

    @Override
    @SneakyThrows
    public void open() {
        if (myCatResultSetEnumerator != null) {
            myCatResultSetEnumerator.close();
        }
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(view.getRelNode().getRowType().getFieldList());
        SqlRecord sqlRecord = context.currentSqlRecord();
        if (inTransaction) {
            onHeap(calciteRowMetaData, sqlRecord);
        } else {
            onParellel(calciteRowMetaData, sqlRecord);
        }
    }

    private void onHeap(CalciteRowMetaData calciteRowMetaData, SqlRecord sqlRecord) {
        TransactionSession transactionSession = context.getTransactionSession();
        LinkedList<RowBaseIterator> iterators = new LinkedList<>();
        for (Map.Entry<String, SqlString> entry : expandToSql.entries()) {
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
                CopyMycatRowMetaData mycatRowMetaData = new CopyMycatRowMetaData(metaData);
                int columnCount = metaData.getColumnCount();
                ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                while (rowIterator.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = rowIterator.getObject(i);
                    }
                    builder.add(row);
                }
                ImmutableList<Object[]> objects1 = builder.build();
                iterators.add(new ResultSetBuilder.DefObjectRowIteratorImpl(mycatRowMetaData, (objects1).iterator()));
            }
        }
        ComposeRowBaseIterator composeRowBaseIterator = new ComposeRowBaseIterator(calciteRowMetaData, iterators);
        MyCatResultSetEnumerator myCatResultSetEnumerator = new MyCatResultSetEnumerator(composeRowBaseIterator);
        this.myCatResultSetEnumerator = myCatResultSetEnumerator;
//        for (Map.Entry<String, SqlString> entry : expandToSql.entries()) {
//            transactionSession.getConnection()
//            MycatConnection mycatConnection = factory.getConnection(target);
//        }
    }

    private void onParellel(CalciteRowMetaData calciteRowMetaData, SqlRecord sqlRecord) throws SQLException {
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        NameableExecutor mycatWorker = mycatWorkerProcessor.getMycatWorker();
        LinkedList<Future<RowBaseIterator>> futureArrayList = new LinkedList<>();
        for (Map.Entry<String, SqlString> entry : expandToSql.entries()) {
            String target = entry.getKey();
            MycatConnection mycatConnection = factory.getConnection(target);
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
        ComposeFutureRowBaseIterator composeFutureRowBaseIterator = new ComposeFutureRowBaseIterator(calciteRowMetaData, futureArrayList);
        this.myCatResultSetEnumerator = new MyCatResultSetEnumerator(composeFutureRowBaseIterator);
    }

    @Override
    public Row next() {
        return myCatResultSetEnumerator.moveNext() ? Row.of(myCatResultSetEnumerator.current()) : null;
    }

    @Override
    public void close() {
        if (myCatResultSetEnumerator != null) {
            myCatResultSetEnumerator.close();
        }
    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }

    public boolean isProxy() {
        return expandToSql.size() == 1;
    }

    public Pair<String, String> getSingleSql() {
        assert isProxy();
        Map.Entry<String, SqlString> stringEntry = expandToSql.entries().iterator().next();
        String key = stringEntry.getKey();
        SqlString value = stringEntry.getValue();
        String psql = value.getSql();
        String sql = apply(psql, params);
        return Pair.of(key, sql);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        for (Map.Entry<String, SqlString> entry : this.expandToSql.entries()) {
            String key = entry.getKey();
            SqlString value = entry.getValue();
            writer.item("targetName:" + key + "->" + value.getSql().replaceAll("\n", " "), "");
            ImmutableList<Integer> dynamicParameters = value.getDynamicParameters();
            if (dynamicParameters != null) {
                writer.item("params", dynamicParameters.stream().map(i -> params.get(i)).collect(Collectors.toList()));
            }

        }
        return explainWriter.ret();
    }
}