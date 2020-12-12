package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableMultimap;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatConnection;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.api.collector.ComposeRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.hbt3.View;
import io.mycat.hbt4.DataSourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.mpp.Row;
import io.mycat.util.Pair;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.apply;
import static io.mycat.hbt4.executor.MycatPreparedStatementUtil.executeQuery;

public class ViewExecutor implements Executor {
    final View view;
    private List<Object> params;
    final DataSourceFactory factory;
    private final ImmutableMultimap<String, SqlString> expandToSql;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ViewExecutor.class);
    public static ViewExecutor create(View view, boolean forUpdate, List<Object> params, DataSourceFactory factory) {
        return new ViewExecutor(view, forUpdate, params, factory);
    }

    protected ViewExecutor(View view, boolean forUpdate, List<Object> params, DataSourceFactory factory) {
        this.view = view;
        this.params = params;
        this.factory = factory;
        this.expandToSql = this.view.expandToSql(forUpdate, params);
        factory.registered(this.expandToSql.keys().asList());
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
        LinkedList<RowBaseIterator> futureArrayList = new LinkedList<>();

        for (Map.Entry<String, SqlString> entry : expandToSql.entries()) {
            MycatConnection mycatConnection = factory.getConnection(entry.getKey());
            Connection connection = mycatConnection.unwrap(Connection.class);
            if (connection.isClosed()){
                LOGGER.error("mycatConnection:{} has closed but still using", mycatConnection);
            }
            futureArrayList.add(
                 executeQuery(connection,mycatConnection, calciteRowMetaData, entry.getValue(), params)
            );
        }
        AtomicBoolean flag = new AtomicBoolean();
        ComposeRowBaseIterator composeFutureRowBaseIterator = new ComposeRowBaseIterator(calciteRowMetaData, futureArrayList);
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
    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        for (Map.Entry<String, SqlString> entry : this.expandToSql.entries()) {
            String key = entry.getKey();
            SqlString value = entry.getValue();
            writer.item("targetName:"+key+"->"+value.getSql().replaceAll("\n"," "),"");
            writer.item("params",params);
        }
        return explainWriter.ret();
    }
}