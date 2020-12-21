package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.api.collector.ComposeRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorCloseCallback;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.hbt4.DataSourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.mpp.Row;
import io.mycat.sqlrecorder.SqlRecord;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.executeQuery;

public class TmpSqlExecutor implements Executor {
    private MycatDataContext context;
    private MycatRowMetaData mycatRowMetaData;
    final String sql;
    final String target;
    final DataSourceFactory factory;
    private List<Object> params;

    public static TmpSqlExecutor create(MycatDataContext context, MycatRowMetaData mycatRowMetaData, String target, String sql, DataSourceFactory factory, List<Object> params) {
        return new TmpSqlExecutor(context, mycatRowMetaData,target,sql, factory,params);
    }

    protected TmpSqlExecutor(MycatDataContext context, MycatRowMetaData mycatRowMetaData, String target, String sql, DataSourceFactory factory, List<Object> params) {
        this.context = context;
        this.mycatRowMetaData = mycatRowMetaData;
        this.sql = sql;
        this.target = target;
        this.factory = factory;
        this.params = params;
        factory.registered(ImmutableList.of(target));
    }

    private MyCatResultSetEnumerator myCatResultSetEnumerator;

    @Override
    @SneakyThrows
    public void open() {
        if (myCatResultSetEnumerator != null) {
            myCatResultSetEnumerator.close();
        }
        MycatRowMetaData calciteRowMetaData =mycatRowMetaData;
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);

        NameableExecutor mycatWorker = mycatWorkerProcessor.getMycatWorker();
        LinkedList<RowBaseIterator> futureArrayList = new LinkedList<>();
        MycatConnection mycatConnection1 = factory.getConnection(target);
        Connection mycatConnection = mycatConnection1.unwrap(Connection.class);
        SqlString sqlString = new SqlString(
                MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(target),
                sql);
        long startTime = SqlRecord.now();
        futureArrayList.add( executeQuery(mycatConnection, mycatConnection1, calciteRowMetaData, sqlString, params, new RowIteratorCloseCallback() {
            @Override
            public void onClose(long rowCount) {
                context.currentSqlRecord().addSubRecord(sqlString,startTime,SqlRecord.now(),target,rowCount);
            }
        }));
        AtomicBoolean flag = new AtomicBoolean();
        ComposeRowBaseIterator composeFutureRowBaseIterator = new ComposeRowBaseIterator(calciteRowMetaData, futureArrayList);
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

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        writer.item("sql",sql);
        writer.item("target",target);
        return explainWriter.ret();
    }
}