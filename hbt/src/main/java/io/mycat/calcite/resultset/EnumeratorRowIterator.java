/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
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
    protected long rowCount;

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
            rowCount++;
            this.currentRow = this.iterator.current();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        Object o = getObject(columnIndex);
        if (wasNull) return null;
        return (LocalDate) o;
    }

    @Override
    public void close() {
        iterator.close();
        if (closeRunnable != null) {
            closeRunnable.run();
        }
    }

    @Override
    public LocalDateTime getTimestamp(int columnIndex) {
        LocalDateTime o = (LocalDateTime)getObject(columnIndex);
        if (wasNull) return null;
        return (LocalDateTime) o;
    }

    @Override
    public Duration getTime(int columnIndex) {
        Duration o = (Duration)getObject(columnIndex);
        if (wasNull) return null;
        return (Duration) o;
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

    public long getRowCount() {
        return rowCount;
    }
}