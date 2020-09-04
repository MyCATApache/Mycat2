package io.mycat.hbt4;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.hbt4.executor.MycatInsertExecutor;
import io.mycat.hbt4.executor.MycatUpdateExecutor;
import io.mycat.hbt4.executor.TempResultSetFactory;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.util.Explains;
import io.mycat.util.Response;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;

public class ResponseExecutorImplementor extends ExecutorImplementorImpl implements ExecutorImplementor {
    protected final Response response;

    public static ResponseExecutorImplementor create(MycatDataContext context, Response response) {
        TempResultSetFactory tempResultSetFactory = new TempResultSetFactoryImpl();
        DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(context);
        return new ResponseExecutorImplementor(datasourceFactory, tempResultSetFactory, response);
    }

    public ResponseExecutorImplementor(
            DatasourceFactory factory,
            TempResultSetFactory tempResultSetFactory,
            Response response) {
        super(factory, tempResultSetFactory);
        this.response = response;
    }

    @Override
    public void implementRoot(MycatRel rel) {
        Executor executor = rel.implement(this);
        try {
            if (executor instanceof MycatInsertExecutor) {
                MycatInsertExecutor insertExecutor = (MycatInsertExecutor) executor;
                runInsert(insertExecutor);
                return;
            }
            if (executor instanceof MycatUpdateExecutor) {
                MycatUpdateExecutor updateExecutor = (MycatUpdateExecutor) executor;
                runUpdate(updateExecutor);
                return;
            }
            runQuery(rel, executor);
        } catch (Exception e) {
            if (executor != null) {
                executor.close();
            }
            response.sendError(e);
        }
        return;
    }

    protected void runQuery(MycatRel rel, Executor executor) {
        RelDataType rowType = rel.getRowType();
        EnumeratorRowIterator rowIterator = new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()),
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
        });
    }

    protected void runUpdate(MycatUpdateExecutor updateExecutor) {
        updateExecutor.open();
        long affectedRow = updateExecutor.affectedRow;
        long lastInsertId = updateExecutor.lastInsertId;
        response.sendOk(lastInsertId, affectedRow);
    }

    protected void runInsert(MycatInsertExecutor insertExecutor) {
        insertExecutor.open();
        long affectedRow = insertExecutor.affectedRow;
        long lastInsertId = insertExecutor.lastInsertId;
        response.sendOk(lastInsertId, affectedRow);
    }
}