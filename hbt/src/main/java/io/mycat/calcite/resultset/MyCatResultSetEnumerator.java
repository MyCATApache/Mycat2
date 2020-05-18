package io.mycat.calcite.resultset;

import io.mycat.api.collector.RowBaseIterator;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyCatResultSetEnumerator<T> implements Enumerator<T> {
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerator.class);

    final AtomicBoolean CANCEL_FLAG;
    final RowBaseIterator rowBaseIterator;
    final int columnCount;
    boolean result = true;
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
        LOGGER
                .debug("-------------------------------" +
               this+
                "------------------");
        LOGGER.debug(Arrays.toString(res));
        return (T) res;
    }

    @Override
    public boolean moveNext() {
        if (CANCEL_FLAG.get()) {
            return false;
        }
        if (result) {
            result = rowBaseIterator.next();
            return result;
        }

        return false;
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