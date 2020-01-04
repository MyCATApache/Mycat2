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
package io.mycat.datasource.jdbc.resultset;

import io.mycat.MycatException;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import java.io.Closeable;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Junwen Chen
 **/
public class JdbcRowBaseIteratorImpl implements RowBaseIterator {

    private final static MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(JdbcRowBaseIteratorImpl.class);
    private final Statement statement;
    private final ResultSet resultSet;
    private final AutoCloseable connection;

    public JdbcRowBaseIteratorImpl(Statement statement, ResultSet resultSet) {
        this(statement, resultSet, null);
    }

    public JdbcRowBaseIteratorImpl(Statement statement, ResultSet resultSet, AutoCloseable connection) {
        this.statement = statement;
        this.resultSet = resultSet;
        this.connection = connection;
    }


    private String toMessage(Exception e) {
        return e.toString();
    }

    @Override
    public MycatRowMetaData metaData() {
        try {
            return new JdbcRowMetaDataImpl(resultSet.getMetaData());
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
            statement.close();
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        if (connection != null) {
            try {
                connection.close();
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
    public Object getObject(int columnIndex) {
        try {
            return resultSet.getObject(columnIndex);
        } catch (Exception e) {
            throw new MycatException(e);
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

    public List<Map<String, Object>> getResultSetMap() {
        return getResultSetMap(this);
    }

    private List<Map<String, Object>> getResultSetMap(JdbcRowBaseIteratorImpl iterator) {
        MycatRowMetaData metaData = iterator.metaData();
        int columnCount = metaData.getColumnCount();
        List<Map<String, Object>> resultList = new ArrayList<>();
        while (iterator.next()) {
            HashMap<String, Object> row = new HashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), iterator.getObject(i));
            }
            resultList.add(row);
        }
        return resultList;
    }

}