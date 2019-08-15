/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.datasource.jdbc.resultset;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.proxy.MySQLPacketUtil;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;

public class TextResultSetResponse extends AbstractMycatResultSetResponse {

  public TextResultSetResponse(RowBaseIterator iterator) {
    super(iterator);
  }

  @Override
  public Iterator<byte[]> rowIterator() {
    final RowBaseIterator rowBaseIterator = iterator;
    final MycatRowMetaData mycatRowMetaData = rowBaseIterator.metaData();
    final TextConvertor convertor = TextConvertorImpl.INSANTCE;
    final int columnCount = mycatRowMetaData.getColumnCount();

    return new Iterator<byte[]>() {
      @Override
      public boolean hasNext() {
        return rowBaseIterator.next();
      }

      @Override
      public byte[] next() {
        byte[][] row = new byte[columnCount][];
        for (int columnIndex = 1, rowIndex = 0; rowIndex < columnCount; columnIndex++, rowIndex++) {
          int columnType = mycatRowMetaData.getColumnType(columnIndex);
          if (rowBaseIterator.wasNull()) {
            row[rowIndex] = null;
            continue;
          }
          row[rowIndex] = getValue(rowBaseIterator, convertor, columnIndex, columnType);
        }
        return MySQLPacketUtil.generateTextRow(row);
      }
    };
  }

  private byte[] getValue(RowBaseIterator rowBaseIterator, TextConvertor convertor, int columnIndex,
      int columnType) {
    byte[] res;
    switch (columnType) {
      case Types.NUMERIC: {

      }
      case Types.DECIMAL: {
        BigDecimal value = rowBaseIterator.getBigDecimal(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor
            .convertBigDecimal(value);
        break;
      }
      case Types.BIT: {
        boolean value = rowBaseIterator.getBoolean(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertBoolean(value);
        break;
      }
      case Types.TINYINT: {
        byte value = rowBaseIterator.getByte(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertByte(value);
        break;
      }
      case Types.SMALLINT: {
        short value = rowBaseIterator.getShort(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertShort(value);
        break;
      }
      case Types.INTEGER: {
        int value = rowBaseIterator.getInt(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertInteger(value);
        break;
      }
      case Types.BIGINT: {
        long value = rowBaseIterator.getLong(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertLong(value);
        break;
      }
      case Types.REAL: {
        float value = rowBaseIterator.getFloat(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertFloat(value);
        break;
      }
      case Types.FLOAT: {

      }
      case Types.DOUBLE: {
        double value = rowBaseIterator.getDouble(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertDouble(value);
        break;
      }
      case Types.BINARY: {

      }
      case Types.VARBINARY: {

      }
      case Types.LONGVARBINARY: {
        byte[] value = rowBaseIterator.getBytes(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertBytes(value);
        break;
      }
      case Types.DATE: {
        Date value = rowBaseIterator.getDate(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertDate(value);
        break;
      }
      case Types.TIME: {
        Time value = rowBaseIterator.getTime(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertTime(value);
        break;
      }
      case Types.TIMESTAMP: {
        Timestamp value = rowBaseIterator.getTimestamp(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        res = convertor.convertTimeStamp(value);
        break;
      }
      case Types.CHAR: {

      }
      case Types.VARCHAR: {

      }
      case Types.LONGVARCHAR: {

      }
      case Types.BLOB: {

      }
      case Types.CLOB: {
        res = rowBaseIterator.getBytes(columnIndex);
        if (rowBaseIterator.wasNull()) {
          return null;
        }
        break;
      }
      case Types.NULL: {
        res = null;
        break;
      }
      default:
        throw new RuntimeException("unsupport!");
    }
    return res;
  }

  @Override
  public void close() throws IOException {
    this.iterator.close();
  }
}