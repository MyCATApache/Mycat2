package io.mycat.calcite.resultset;

import io.mycat.api.collector.AbstractObjectRowIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.calcite.linq4j.Enumerator;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author chen junwen
 */
public class EnumeratorRowIterator extends AbstractObjectRowIterator {
    protected final MycatRowMetaData mycatRowMetaData;
    protected final Enumerator<Object[]> iterator;
    protected final Runnable closeRunnable;
    public EnumeratorRowIterator(MycatRowMetaData mycatRowMetaData, Enumerator<Object[]> iterator){
        this(mycatRowMetaData,iterator,null);
    }
    public EnumeratorRowIterator(MycatRowMetaData mycatRowMetaData, Enumerator<Object[]> iterator,Runnable closeRunnale) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.iterator = iterator;
        this.closeRunnable = closeRunnale;
    }

    @Override
    public MycatRowMetaData getMetaData() {
        return mycatRowMetaData;
    }

    @Override
    public boolean next() {
        if (this.iterator.moveNext()) {
            this.currentRow = this.iterator.current();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Date getDate(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        if (o instanceof Integer) {
            return new Date((Integer) o);
        }
        if (o instanceof Long) {
            return new Date((Long) o);
        }
        return (Date) o;
    }

    @Override
    public void close() {
        iterator.close();
        if (closeRunnable !=null){
            closeRunnable.run();
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        if (o instanceof Integer) {
            return new Timestamp((Integer) o);
        }
        if (o instanceof Long) {
            return new Timestamp((Long) (o));
        }
        return (Timestamp) o;
    }

    @Override
    public Time getTime(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        if (o instanceof Integer) {
            return new Time((Integer) o);
        }
        if (o instanceof Long) {
            return new Time((Long) (o));
        }
        return (Time) o;
    }

}