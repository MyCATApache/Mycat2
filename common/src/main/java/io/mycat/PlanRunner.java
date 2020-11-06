package io.mycat;

import io.mycat.api.collector.RowBaseIterator;

import java.util.List;
/**
 * @author Junwen Chen
 **/
public interface PlanRunner {
    List<String> explain();

    RowBaseIterator run();

}