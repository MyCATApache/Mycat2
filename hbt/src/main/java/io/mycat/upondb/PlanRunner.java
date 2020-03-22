package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;

import java.util.List;

public interface PlanRunner {
    List<String> explain();

    RowBaseIterator run();

}