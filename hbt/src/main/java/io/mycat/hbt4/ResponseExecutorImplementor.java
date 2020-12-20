package io.mycat.hbt4;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.hbt4.executor.MycatInsertExecutor;
import io.mycat.hbt4.executor.MycatUpdateExecutor;
import io.mycat.hbt4.executor.TempResultSetFactory;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.util.Explains;
import io.mycat.util.Response;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ResponseExecutorImplementor extends ExecutorImplementorImpl implements ExecutorImplementor {
    private final MycatDataContext context;
    protected final Response response;

    public static ResponseExecutorImplementor create(MycatDataContext context, Response response, DataSourceFactory datasourceFactory) {
        TempResultSetFactory tempResultSetFactory = new TempResultSetFactoryImpl();
        TransactionType transactionType = context.getTransactionSession().transactionType();
        switch (transactionType) {
            case PROXY_TRANSACTION_TYPE:
                return ProxyExecutorImplementor.create(context, response);
            default:
            case JDBC_TRANSACTION_TYPE:
                return new ResponseExecutorImplementor(context,datasourceFactory, tempResultSetFactory, response);
        }

    }

    public ResponseExecutorImplementor(
            MycatDataContext context,
            DataSourceFactory factory,
            TempResultSetFactory tempResultSetFactory,
            Response response) {
        super(context,factory, tempResultSetFactory);
        this.context = context;
        this.response = response;
    }

    @SneakyThrows
    @Override
    public void implementRoot(MycatRel rel, List<String> aliasList) {
        Objects.requireNonNull(rel);
        Executor executor = Objects.requireNonNull(
                rel.implement(this)
        );
        try {
            if (executor instanceof MycatInsertExecutor) {
                MycatInsertExecutor insertExecutor = (MycatInsertExecutor) executor;
                factory.open();
                runInsert(insertExecutor);
                return;
            }
            if (executor instanceof MycatUpdateExecutor) {
                MycatUpdateExecutor updateExecutor = (MycatUpdateExecutor) executor;
                factory.open();
                runUpdate(updateExecutor);
                return;
            }
            runQuery(rel, executor, aliasList);
        } catch (Exception e) {
            if (executor != null) {
                executor.close();
            }
            response.sendError(e);
        }finally {
            factory.close();
        }
        return;
    }

    protected void runQuery(MycatRel rel, Executor executor, List<String> aliasList) {
        RelDataType rowType = rel.getRowType();

        CalciteRowMetaData calciteRowMetaData = aliasList!=null?
                new CalciteRowMetaData(rowType.getFieldList(), aliasList)
                : new CalciteRowMetaData(rowType.getFieldList());
        EnumeratorRowIterator rowIterator = new EnumeratorRowIterator(
                calciteRowMetaData,
                Linq4j.asEnumerable(() -> executor.outputObjectIterator()).enumerator(), () -> {
        });
        response.sendResultSet(new RowIterable(rowIterator) {
            @Override
            public RowBaseIterator get() {
                factory.open();
                executor.open();
                return super.get();
            }

            @Override
            public RowBaseIterator explain() {
                String mycatRelNodeText = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(rel);
                List<String> explain = Explains.explain(null, null, null, null, mycatRelNodeText);
                ResultSetBuilder builder = ResultSetBuilder.create();
                builder.addColumnInfo("plan", JDBCType.VARCHAR);
                explain.forEach(i -> builder.addObjectRowPayload(Arrays.asList(i)));
                return builder.build();
            }

            @Override
            public void close() throws IOException {
                long rowCount = rowIterator.getRowCount();
                SqlRecord sqlRecord = context.currentSqlRecord();
                sqlRecord.setSqlRows(rowCount);
                super.close();
            }
        });
    }

    protected void runUpdate(MycatUpdateExecutor updateExecutor) {
        updateExecutor.open();
        long affectedRow = updateExecutor.getAffectedRow();
        long lastInsertId = updateExecutor.getLastInsertId();
        SqlRecord sqlRecord = context.currentSqlRecord();
        sqlRecord.setSqlRows(affectedRow);
        response.sendOk(lastInsertId, affectedRow);
    }

    protected void runInsert(MycatInsertExecutor insertExecutor) {
        insertExecutor.open();
        long affectedRow = insertExecutor.affectedRow;
        long lastInsertId = insertExecutor.lastInsertId;
        SqlRecord sqlRecord = context.currentSqlRecord();
        sqlRecord.setSqlRows(affectedRow);
        response.sendOk(lastInsertId, affectedRow);
    }
}