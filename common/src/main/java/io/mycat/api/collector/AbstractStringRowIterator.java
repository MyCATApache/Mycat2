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
package io.mycat.api.collector;

import io.mycat.MycatTimeUtil;
import io.mycat.beans.mycat.MycatRowMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;

/**
 * chen junwen
 * <p>
 * a iterator,transform text to object
 */
public abstract class AbstractStringRowIterator implements RowBaseIterator {

    private static final Logger logger = LoggerFactory.getLogger(AbstractStringRowIterator.class);
    protected final MycatRowMetaData mycatRowMetaData;
    protected final Iterator<String[]> iterator;
    private String[] currentRow;
    private boolean wasNull;

    public AbstractStringRowIterator(MycatRowMetaData mycatRowMetaData, Iterator<String[]> iterator) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.iterator = iterator;
    }

    @Override
    public MycatRowMetaData getMetaData() {
        return mycatRowMetaData;
    }

    @Override
    public boolean next() {
        if (this.iterator.hasNext()) {
            this.currentRow = this.iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        return o;
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return false;
        return Boolean.parseBoolean(o);
    }

    @Override
    public byte getByte(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return 0;
        return Byte.parseByte(o);
    }

    @Override
    public short getShort(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return 0;
        return Short.parseShort(o);
    }

    @Override
    public int getInt(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return 0;
        return Integer.parseInt(o);
    }

    @Override
    public long getLong(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return 0;
        return Long.parseLong(o);
    }

    @Override
    public float getFloat(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return 0;
        return Float.parseFloat(o);
    }

    @Override
    public double getDouble(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return 0;
        return Double.parseDouble(o);
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return null;
        return o.getBytes();
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return null;
        return LocalDate.parse(o);
    }

    @Override
    public Duration getTime(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return null;
        return MycatTimeUtil.timeStringToTimeDuration(o);
    }

    @Override
    public LocalDateTime getTimestamp(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return null;
        return (LocalDateTime) MycatTimeUtil.timestampStringToTimestamp(o);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return null;
        return new ByteArrayInputStream(o.getBytes());
    }

    private String getString(String o1) {
        String o = o1;
        wasNull = o == null;
        return o;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        String o = getString(currentRow[columnIndex ]);
        if (wasNull) return null;
        return new BigDecimal(o);
    }

    @Override
    public Object getObject(int columnIndex) {
        int columnType = mycatRowMetaData.getColumnType(columnIndex);
        switch (columnType) {
            case Types.NUMERIC: {

            }
            case Types.DECIMAL: {
                return this.getBigDecimal(columnIndex);
            }
            case Types.BIT: {
                return this.getBoolean(columnIndex);
            }
            case Types.TINYINT: {
                return this.getByte(columnIndex);
            }
            case Types.SMALLINT: {
                return this.getShort(columnIndex);
            }
            case Types.INTEGER: {
                return this.getInt(columnIndex);
            }
            case Types.BIGINT: {
                return this.getLong(columnIndex);
            }
            case Types.REAL: {
                return this.getFloat(columnIndex);
            }
            case Types.FLOAT: {

            }
            case Types.DOUBLE: {
                return this.getDouble(columnIndex);
            }
            case Types.BINARY: {

            }
            case Types.VARBINARY: {

            }
            case Types.LONGVARBINARY: {
                return this.getBytes(columnIndex);
            }
            case Types.DATE: {
                return this.getDate(columnIndex);
            }
            case Types.TIME: {
                return this.getTime(columnIndex);
            }
            case Types.TIMESTAMP: {
                return this.getTimestamp(columnIndex);
            }
            case Types.CLOB:
            case Types.CHAR: {

            }
            case Types.VARCHAR: {

            }
            case Types.LONGVARCHAR: {
                return this.getString(columnIndex);
            }
            case Types.BLOB:
          {
                return this.getBytes(columnIndex);
            }
            case Types.NULL: {
                return null;
            }
            default:
                throw new RuntimeException("unsupport!");
        }
    }

}