package io.mycat.calcite.prepare;

import io.mycat.api.collector.RowBaseIterator;

import java.util.List;

public interface PlanRunner {
    public List<String> explain();

    RowBaseIterator run();
}