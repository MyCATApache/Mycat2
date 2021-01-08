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

import io.mycat.MycatConnection;
import io.mycat.MycatException;
import io.mycat.MycatTimeUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIteratorCloseCallback;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

import static java.sql.Types.*;

/**
 * @author Junwen Chen
 **/
public class JdbcRowBaseIterator implements RowBaseIterator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRowBaseIterator.class);
    private final Statement statement;
    private final ResultSet resultSet;
    private final String sql;
    private final RowIteratorCloseCallback closeCallback;
    private MycatRowMetaData metaData;
    private MycatConnection connection;
    private long rowCount = 0;
    private boolean hasNext = true;

    @SneakyThrows
    public JdbcRowBaseIterator(MycatRowMetaData metaData, MycatConnection connection, Statement statement, ResultSet resultSet, RowIteratorCloseCallback closeCallback, String sql) {
        this.connection = connection;
        this.sql = sql;
        this.metaData = metaData == null ? new JdbcRowMetaData(resultSet.getMetaData()) : metaData;
        ;
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
            return this.metaData;
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean next() {
        try {
            if (hasNext) {
                if (hasNext = resultSet.next()) {
                    rowCount += 1;
                    return true;
                } else {
                    return false;
                }
            }
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
        return false;
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
                closeCallback.onClose(rowCount);
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
            return resultSet.getString(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        try {
            return resultSet.getBoolean(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public byte getByte(int columnIndex) {
        try {
            return resultSet.getByte(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public short getShort(int columnIndex) {
        try {
            return resultSet.getShort(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public int getInt(int columnIndex) {
        try {
            return resultSet.getInt(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public long getLong(int columnIndex) {
        try {
            return resultSet.getLong(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public float getFloat(int columnIndex) {
        try {
            return resultSet.getFloat(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public double getDouble(int columnIndex) {
        try {
            return resultSet.getDouble(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        try {
            return resultSet.getBytes(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        try {
            Date date = resultSet.getDate(columnIndex+1);
            if (date != null) {
                return date.toLocalDate();
            }
            return null;
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public Duration getTime(int columnIndex) {
        try {
            String string = resultSet.getString(columnIndex+1);
            if (string == null) {
                return null;
            }
            return MycatTimeUtil.timeStringToTimeDuration(string);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public LocalDateTime getTimestamp(int columnIndex) {
        try {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex+1);
            if (timestamp == null) {
                return null;
            }
            return timestamp.toLocalDateTime();
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        try {
            return resultSet.getAsciiStream(columnIndex+1);
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        try {
            return resultSet.getBinaryStream(columnIndex+1);
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
        columnIndex = columnIndex+1;
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
                double aDouble = resultSet.getDouble(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aDouble;
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
                return b ? null : string;
            }
            case VARCHAR: {
                String string = resultSet.getString(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : string;
            }
            case LONGVARCHAR: {
                String string = resultSet.getString(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : string;
            }
            case DATE: {
                Date date = resultSet.getDate(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : date.toLocalDate();
            }
            case TIME_WITH_TIMEZONE:
            case TIME: {
                String time = resultSet.getString(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : MycatTimeUtil.timeStringToTimeDuration(time);
            }
            case TIMESTAMP_WITH_TIMEZONE:
            case TIMESTAMP: {
                Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : timestamp.toLocalDateTime();
            }
            case BINARY: {
                byte[] bytes = resultSet.getBytes(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : bytes;
            }
            case VARBINARY: {
                byte[] bytes = resultSet.getBytes(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : bytes;
            }
            case LONGVARBINARY: {
                byte[] bytes = resultSet.getBytes(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : bytes;
            }
            case NULL: {
                return null;
            }
            case BOOLEAN: {
                boolean aBoolean = resultSet.getBoolean(columnIndex);
                boolean b = resultSet.wasNull();
                return b ? null : aBoolean;
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