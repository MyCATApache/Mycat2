package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;

import java.io.Closeable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-22 01:17
 * a simple proxy collector as map
 **/
public interface RowBaseIterator extends Closeable, BaseIterator {

    MycatRowMetaData getMetaData();

    boolean next();

    void close();

    boolean wasNull();

    String getString(int columnIndex);

    boolean getBoolean(int columnIndex);

    byte getByte(int columnIndex);

    short getShort(int columnIndex);

    int getInt(int columnIndex);

    long getLong(int columnIndex);

    float getFloat(int columnIndex);

    double getDouble(int columnIndex);

    byte[] getBytes(int columnIndex);

    LocalDate getDate(int columnIndex);

    Duration getTime(int columnIndex);

    LocalDateTime getTimestamp(int columnIndex);

    java.io.InputStream getAsciiStream(int columnIndex);

    java.io.InputStream getBinaryStream(int columnIndex);

    Object getObject(int columnIndex);

    default Object[] getObjects(int count) {
        Object[] objects = new Object[count];
        for (int i = 0; i < count; i++) {
            objects[i] = getObject(i);
        }
        return objects;
    }

    BigDecimal getBigDecimal(int columnIndex);

    public default List<Map<String, Object>> getResultSetMap() {
        return getResultSetMap(this);
    }

    public default List<Map<String, Object>> getResultSetMap(RowBaseIterator iterator) {
        MycatRowMetaData metaData = iterator.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<Map<String, Object>> resultList = new ArrayList<>();
        while (iterator.next()) {
            HashMap<String, Object> row = new HashMap<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                row.put(metaData.getColumnName(i), iterator.getObject(i));
            }
            resultList.add(row);
        }
        return resultList;
    }

}