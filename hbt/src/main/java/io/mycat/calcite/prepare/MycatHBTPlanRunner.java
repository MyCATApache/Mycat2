package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.MycatCalciteDataContext;

import java.util.List;
import java.util.function.Supplier;

public class MycatHBTPlanRunner implements PlanRunner {
    @Override
    public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
        return null;
    }

    @Override
    public List<String> explain() {
        return null;
    }
}