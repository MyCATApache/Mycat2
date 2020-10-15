package io.mycat.calcite.resultset;

import io.mycat.MycatException;
import io.mycat.api.collector.AbstractObjectRowIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Enumerator;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;


/**
 * @author chen junwen
 */
public class EnumeratorRowIterator extends AbstractObjectRowIterator {
    protected final MycatRowMetaData mycatRowMetaData;
    protected final Enumerator<Object[]> iterator;
    protected final Runnable closeRunnable;

    public EnumeratorRowIterator(MycatRowMetaData mycatRowMetaData, Enumerator<Object[]> iterator) {
        this(mycatRowMetaData, iterator, null);
    }

    public EnumeratorRowIterator(MycatRowMetaData mycatRowMetaData, Enumerator<Object[]> iterator, Runnable closeRunnale) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.iterator = iterator;
        this.closeRunnable = closeRunnale;
    }

    @Override
    public MycatRowMetaData getMetaData() {
        return mycatRowMetaData;
    }

    @Override
    public String getString(int columnIndex) {
        Object object = getObject(columnIndex);
        if (object instanceof String){
            return (String)object;
        }
        if (object instanceof Duration){
            Duration duration = (Duration) object;
            int SECONDS_PER_HOUR = 60*60;
            int SECONDS_PER_MINUTE = 60;
            long hours = duration.getSeconds()/ SECONDS_PER_HOUR;
            int minutes = (int) ((duration.getSeconds() % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
            int secs = (int) (duration.getSeconds() % SECONDS_PER_MINUTE);
            int nano = duration.getNano();
            if (nano == 0) {
                return String.format("%d:%02d:%02d",hours,minutes,secs);
            }
            return String.format("%d:%02d:%02d.%09d",hours,minutes,secs,nano);
        }
        return super.getString(columnIndex);
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
            return Date.valueOf(LocalDate.ofEpochDay((Integer) o));
        }
        if (o instanceof Long) {
            return new Date((Long) o);
        }
        if (o instanceof LocalDate) {
            return Date.valueOf((LocalDate) o);
        }
        return (Date) o;
    }

    @Override
    public void close() {
        iterator.close();
        if (closeRunnable != null) {
            closeRunnable.run();
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        if (o instanceof Integer) {
            return Timestamp.valueOf(LocalDate.ofEpochDay((Integer) o).atStartOfDay());
        }
        if (o instanceof Long) {
            return new Timestamp((Long) (o));
        }
        if (o instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) o);
        }

        return (Timestamp) o;
    }

    @Override
    public Time getTime(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        if (o instanceof Duration) {
            Duration o1 = (Duration) o;
            long seconds = o1.getSeconds();
            int nano = o1.getNano();

            if (seconds>=0){
                Time time = new Time(Instant.ofEpochSecond(seconds, nano).toEpochMilli());
                if (time.getHours()>838){
                    throw new MycatException("Truncated incorrect time value:"+time.getHours());
                }
                return time;
            }
           throw new UnsupportedOperationException();
        }
        if (o instanceof Integer) {
            throw new UnsupportedOperationException();
        }
        if (o instanceof Long) {
            return new Time((Long) (o));
        }
        return (Time) o;
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        if (o instanceof ByteString) {
            return ((ByteString) o).getBytes();
        }
        if (o instanceof byte[]) {
            return (byte[]) o;
        }
        throw new UnsupportedOperationException();
    }
}