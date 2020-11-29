package io.mycat.hbt4.executor;

import io.mycat.hbt3.IndexScan;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.rel.RelNode;

import java.util.List;

public class IndexScanExecutor  implements Executor {
    private final IndexScan indexScan;
    private final List<Object> params;

    public IndexScanExecutor(IndexScan indexScan, List<Object> params) {
        this.indexScan = indexScan;
        this.params = params;

        RelNode input = indexScan.getInput();
    }

    @Override
    public void open() {

    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}
