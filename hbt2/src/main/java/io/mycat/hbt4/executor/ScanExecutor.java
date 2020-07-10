package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ScanExecutor implements Executor {

    private Iterator<Row> iter;

    @Override
    public void open() {
        List<Object[]> objects = Arrays.asList(new Object[]{1L},new Object[]{2L});
        this.iter = objects.stream().map(i->new Row(i)).iterator();
    }

    @Override
    public Row next() {
        boolean b = iter.hasNext();
        if (b){
            return iter.next();
        }
        return null;
    }

    @Override
    public void close() {

    }
}