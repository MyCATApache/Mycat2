package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.EmptyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.PlanRunner;

import java.util.Arrays;
import java.util.List;

public abstract class SimpleSQLPrepareObjectPlanner extends MycatSQLPrepareObject implements PlanRunner {

    public SimpleSQLPrepareObjectPlanner(MycatDBContext uponDBContext, String sql) {
        super(null,uponDBContext, sql,false);
    }

    public abstract void innerEun();

    @Override
    public RowBaseIterator run() {
        innerEun();
        return new UpdateRowIteratorResponse(0,0,uponDBContext.getServerStatus());
    }

    @Override
    public PlanRunner plan(List<Object> params) {
        return this;
    }

    public abstract String innerExplain();

    @Override
    public List<String> explain() {
        return Arrays.asList(getClass().getSimpleName());
    }

    @Override
    public MycatRowMetaData prepareParams() {
        return EmptyMycatRowMetaData.INSTANCE;
    }

    @Override
    public MycatRowMetaData resultSetRowType() {
        return UpdateRowMetaData.INSTANCE;
    }

}