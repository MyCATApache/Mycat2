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

/**
 * @author Junwen Chen
 **/
public class JdbcRowBaseIterator implements RowBaseIterator {

    private final static MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JdbcRowBaseIterator.class);
    private final Statement statement;
    private final ResultSet resultSet;
    private final AutoCloseable closeCallback;

    public JdbcRowBaseIterator(Statement statement, ResultSet resultSet) {
        this(statement, resultSet, null);
    }

    private JdbcRowBaseIterator(Statement statement, ResultSet resultSet, AutoCloseable closeCallback) {
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
            return new JdbcRowMetaData(resultSet.getMetaData());
        } catch (Exception e) {
            throw new MycatException(toMessage(e));
        }
    }

    @Override
    public boolean next() {
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
        Object object = resultSet.getObject(columnIndex);
        return object;
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