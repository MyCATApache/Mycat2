package io.mycat.hbt4.executor;

import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Iterator;

public class MycatJdbcExecutor implements Executor {


    private MycatSQLTableScan tableScan;
    private Iterator<Object[]> iterator;

    public MycatJdbcExecutor(MycatSQLTableScan tableScan) {
        this.tableScan = tableScan;
    }

    @Override
    public void open() {
        this.iterator = tableScan.scan(null).iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return Row.of(iterator.next());
        } else {
            return null;
        }
    }


    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}