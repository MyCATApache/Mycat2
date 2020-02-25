package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.MycatCalciteDataContext;

import java.util.List;
import java.util.function.Supplier;

public interface PlanRunner {
    public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext);
    public List<String> explain();
}