package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableMultimap;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatConnection;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.api.collector.ComposeFutureRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.hbt3.View;
import io.mycat.hbt4.DatasourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import io.mycat.util.Pair;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.apply;
import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.executeQuery;

public class ViewExecutor implements Executor {
    final View view;
    private List<Object> params;
    final DatasourceFactory factory;
    private final ImmutableMultimap<String, SqlString> expandToSql;

    public static ViewExecutor create(View view, boolean forUpdate, List<Object> params, DatasourceFactory factory) {
        return new ViewExecutor(view, forUpdate, params, factory);
    }

    protected ViewExecutor(View view, boolean forUpdate, List<Object> params, DatasourceFactory factory) {
        this.view = view;
        this.params = params;
        this.factory = factory;
        this.expandToSql = this.view.expandToSql(forUpdate, params);
        factory.regist(this.expandToSql.keys().asList());
    }

    private MyCatResultSetEnumerator myCatResultSetEnumerator;

    @Override
    @SneakyThrows
    public void open() {
        if (myCatResultSetEnumerator != null) {
            myCatResultSetEnumerator.close();
        }
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(view.getRelNode().getRowType().getFieldList());
        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        NameableExecutor mycatWorker = mycatWorkerProcessor.getMycatWorker();
        LinkedList<Future<RowBaseIterator>> futureArrayList = new LinkedList<>();

        for (Map.Entry<String, SqlString> entry : expandToSql.entries()) {
            Connection mycatConnection = factory.getConnection(entry.getKey());
            futureArrayList.add(mycatWorker.submit(() -> executeQuery(mycatConnection, calciteRowMetaData, entry.getValue(), params)));
        }
        AtomicBoolean flag = new AtomicBoolean();
        ComposeFutureRowBaseIterator composeFutureRowBaseIterator = new ComposeFutureRowBaseIterator(calciteRowMetaData, futureArrayList);
        this.myCatResultSetEnumerator = new MyCatResultSetEnumerator(flag, composeFutureRowBaseIterator);
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
}