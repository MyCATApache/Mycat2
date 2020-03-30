/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.beans.mycat;

import io.mycat.MycatException;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import lombok.SneakyThrows;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Objects;

import static java.sql.Types.*;

/**
 * @author Junwen Chen
 **/
public class JdbcRowBaseIterator implements RowBaseIterator {

    private final static MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JdbcRowBaseIterator.class);
    private MycatRowMetaData metaData;
    private final Statement statement;
    private final ResultSet resultSet;
    private final String sql;
    private final AutoCloseable closeCallback;

    @SneakyThrows
    public JdbcRowBaseIterator(MycatRowMetaData metaData, Statement statement, ResultSet resultSet, AutoCloseable closeCallback,String sql) {
        this.sql = sql;
        this.metaData = metaData != null ? metaData : new JdbcRowMetaData(resultSet.getMetaData());
        if (this.metaData.getColumnCount()!=resultSet.getMetaData().getColumnCount()){
            throw new AssertionError();
        }
        this.statement = statement;
        this.resultSet = Objects.requireNonNull(resultSet);
        this.closeCallback = closeCallback;
    }


    private String toMessage(Exception e) {
        return e.toString();
    }

    @Override
    public MycatRowMetaData getMetaData() {
        try {
            return metaData;
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean next() {
        MycatRowMetaData metaData = getMetaData();
        try {
            return resultSet.next();
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public void close() {
        try {
            resultSet.close();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        if (closeCallback != null) {
            try {
                closeCallback.close();
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
    }

    @Override
    public boolean wasNull() {
        try {
            return resultSet.wasNull();
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public String getString(int columnIndex) {
        try {
            return resultSet.getString(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        try {
            return resultSet.getBoolean(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public byte getByte(int columnIndex) {
        try {
            return resultSet.getByte(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public short getShort(int columnIndex) {
        try {
            return resultSet.getShort(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public int getInt(int columnIndex) {
        try {
            return resultSet.getInt(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public long getLong(int columnIndex) {
        try {
            return resultSet.getLong(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public float getFloat(int columnIndex) {
        try {
            return resultSet.getFloat(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public double getDouble(int columnIndex) {
        try {
            return resultSet.getDouble(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        try {
            return resultSet.getBytes(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public Date getDate(int columnIndex) {
        try {
            return resultSet.getDate(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public Time getTime(int columnIndex) {
        try {
            return resultSet.getTime(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        try {
            return resultSet.getTimestamp(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        try {
            return resultSet.getAsciiStream(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        try {
            return resultSet.getBinaryStream(columnIndex);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    @SneakyThrows
    public Object getObject(int columnIndex) {
        ResultSetMetaData origin = resultSet.getMetaData();
        MycatRowMetaData metaData = getMetaData();//该方法可能被重写
        int columnType = metaData.getColumnType(columnIndex);
        switch (columnType) {
            case BIT: {
                boolean aBoolean = resultSet.getBoolean(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aBoolean;
            }
            case TINYINT: {
                byte aByte = resultSet.getByte(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aByte;
            }
            case SMALLINT: {
                short aShort = resultSet.getShort(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aShort;
            }
            case INTEGER: {
                int anInt = resultSet.getInt(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : anInt;
            }
            case BIGINT: {
                long aLong = resultSet.getLong(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aLong;
            }
            case FLOAT: {
                float aFloat = resultSet.getFloat(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aFloat;
            }
            case REAL: {
                float aFloat = resultSet.getFloat(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aFloat;
            }
            case DOUBLE: {
                double aDouble = resultSet.getDouble(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aDouble;
            }
            case NUMERIC: {
                BigDecimal bigDecimal = resultSet.getBigDecimal(columnIndex);//review
                boolean b = resultSet.wasNull();
                return b ? null : bigDecimal;
            }
            case DECIMAL: {
                BigDecimal bigDecimal = resultSet.getBigDecimal(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : bigDecimal;
            }
            case CHAR: {
                String string = resultSet.getString(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:string;
            }
            case VARCHAR: {
                String string = resultSet.getString(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:string;
            }
            case LONGVARCHAR: {
                String string = resultSet.getString(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:string;
            }
            case DATE: {
                Date date = resultSet.getDate(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:date;
            }
            case TIME: {
                Time time = resultSet.getTime(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:time;
            }
            case TIMESTAMP: {
                Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:timestamp;
            }
            case BINARY: {
                byte[] bytes = resultSet.getBytes(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:bytes;
            }
            case VARBINARY: {
                byte[] bytes = resultSet.getBytes(columnIndex);
                boolean b = resultSet.wasNull();
                return b?null:bytes;
            }
            case LONGVARBINARY: {
                byte[] bytes = resultSet.getBytes(columnIndex);
                boolean b = resultSet.wasNull();
                return  b?null:bytes;
            }
            case NULL: {
                return null;
            }
            case BOOLEAN: {
                boolean aBoolean = resultSet.getBoolean(columnIndex);
                boolean b = resultSet.wasNull();
                return  b?null:aBoolean;
            }

            case TIME_WITH_TIMEZONE: {
                Time time = resultSet.getTime(columnIndex);
                boolean b = resultSet.wasNull();
                return  b?null:time;
            }
            case TIMESTAMP_WITH_TIMEZONE: {
                Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                boolean b = resultSet.wasNull();
                return  b?null:timestamp;
            }
            case ROWID:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case NCLOB:
            case SQLXML:
            case REF_CURSOR:
            case OTHER:
            case JAVA_OBJECT:
            case DISTINCT:
            case STRUCT:
            case ARRAY:
            case BLOB:
            case CLOB:
            case REF:
            case DATALINK:
            default:
                LOGGER.warn("may be unsupported type :" + JDBCType.valueOf(columnType));
                return resultSet.getObject(columnIndex);
        }
    }


    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        try {
            return resultSet.getBigDecimal(columnIndex);
        } catch (Exception e) {
            throw new MycatException(e);
        }
    }


}