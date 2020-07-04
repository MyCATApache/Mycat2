package io.mycat.hbt2.nosql;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;

/**
 * nosql值
 * @author wangzihaogithub
 * 2020年6月9日 00:39:46
 */
public interface NosqlValue {

    boolean wasNull();

    Collection<NosqlValue> getCollection();

    Map<String,NosqlValue> getMap();

    String getString();

    boolean getBoolean();

    byte getByte();

    short getShort();

    int getInt();

    long getLong();

    float getFloat();

    double getDouble();

    byte[] getBytes();

    java.sql.Date getDate();

    java.sql.Time getTime();

    java.sql.Timestamp getTimestamp();

    java.io.InputStream getAsciiStream();

    java.io.InputStream getBinaryStream();

    Object getObject();

    BigDecimal getBigDecimal();

}
