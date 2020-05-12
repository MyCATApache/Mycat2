package io.mycat.calcite.resultset;

import io.mycat.api.collector.RowBaseIterator;
import org.apache.calcite.linq4j.Enumerator;

import java.sql.Date;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyCatResultSetEnumerator<T> implements Enumerator<T> {
    final AtomicBoolean CANCEL_FLAG;
    final RowBaseIterator rowBaseIterator;
    final int columnCount;

    public MyCatResultSetEnumerator(AtomicBoolean CANCEL_FLAG, RowBaseIterator rowBaseIterator) {
        this.CANCEL_FLAG = CANCEL_FLAG;
        this.rowBaseIterator = rowBaseIterator;
        this.columnCount = rowBaseIterator.getMetaData().getColumnCount();
    }


    @Override
    public T current() {
        Object[] res = new Object[columnCount];
        for (int i = 0, j = 1; i < columnCount; i++, j++) {
            Object object = rowBaseIterator.getObject(j);
            if (object instanceof Date) {
                res[i] = ((Date) object).getTime();
            } else {
                res[i] = object;
            }
        }
        return (T) res;
    }

    @Override
    public boolean moveNext() {
        if (CANCEL_FLAG.get()) {
            return false;
        }
        boolean result = rowBaseIterator.next();
        if (result) {
            return result;
        }
        return result;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (rowBaseIterator != null) {
            rowBaseIterator.close();
        }
    }
}