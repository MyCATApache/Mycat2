package io.mycat.router.util;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;

import java.util.Iterator;
import java.util.Objects;
import java.util.StringJoiner;
/**
 * junwen 294712221@qq.com
 */
public class RowIteratorToInsertSQL implements Iterator<String> {
    final String tableName;
    final RowBaseIterator iterator;
    final MycatRowMetaData metaData;
    private final String header;
    private int batchSize;
    final int columnCount;
    private boolean hasNext;

    public RowIteratorToInsertSQL(String tableName, RowBaseIterator iterator, int batchSize) {
        this.tableName = tableName;
        this.iterator = iterator;
        this.metaData = iterator.getMetaData();
        this.batchSize = batchSize;
        this.columnCount = this.metaData.getColumnCount();
        StringJoiner joiner = new StringJoiner("`,`", "INSERT INTO " + tableName + " (`", "`) VALUES ");
        for (int i = 0; i < this.columnCount; i++) {
            joiner.add(this.metaData.getColumnName(i));
        }
        this.header = joiner.toString();
    }

    @Override
    public boolean hasNext() {
        if (!this.hasNext) {
            this.hasNext = iterator.next();
        }
        return this.hasNext;
    }

    @Override
    public String next() {
        StringJoiner wrapper = new StringJoiner(",", this.header, ";");
        for (int counter = 0; hasNext() && counter < batchSize; counter++) {
            this.hasNext = false;
            StringBuilder values = new StringBuilder();
            values.append("(");
            for (int i = 1; i <= columnCount; i++) {
                Object object = iterator.getObject(i);
                String value = Objects.toString(object);
                values.append('\'').append( value).append('\'');
            }
            values.append(")");
            wrapper.add(values);
        }
        return wrapper.toString();
    }
}