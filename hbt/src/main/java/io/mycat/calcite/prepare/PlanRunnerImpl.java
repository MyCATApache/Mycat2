package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.beans.mycat.EmptyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.MycatCalciteDataContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public abstract class PlanRunnerImpl extends MycatSQLPrepareObject implements PlanRunner {

    public PlanRunnerImpl(String defaultSchemaName, String sql) {
        super(defaultSchemaName, sql);
    }

    public abstract void innerEun(MycatCalciteDataContext dataContext);

    @Override
    public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
        return () -> {
            innerEun(dataContext);
            return UpdateRowIterator.EMPTY;
        };
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

    @Override
    public PlanRunner plan(List<Object> params) {
        return this;
    }

}