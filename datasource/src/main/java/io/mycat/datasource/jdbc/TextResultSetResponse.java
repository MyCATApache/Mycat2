package io.mycat.datasource.jdbc;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.mysqlapi.collector.RowBaseIterator;
import io.mycat.proxy.MySQLPacketUtil;
import java.io.IOException;
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
        res = convertor
            .convertBigDecimal(rowBaseIterator.getBigDecimal(columnIndex));
        break;
      }
      case Types.BIT: {
        res = convertor.convertBoolean(rowBaseIterator.getBoolean(columnIndex));
        break;
      }
      case Types.TINYINT: {
        res = convertor.convertByte(rowBaseIterator.getByte(columnIndex));
        break;
      }
      case Types.SMALLINT: {
        res = convertor.convertShort(rowBaseIterator.getShort(columnIndex));
        break;
      }
      case Types.INTEGER: {
        res = convertor.convertInteger(rowBaseIterator.getInt(columnIndex));
        break;
      }
      case Types.BIGINT: {
        res = convertor.convertLong(rowBaseIterator.getLong(columnIndex));
        break;
      }
      case Types.REAL: {
        res = convertor.convertFloat(rowBaseIterator.getFloat(columnIndex));
        break;
      }
      case Types.FLOAT: {

      }
      case Types.DOUBLE: {
        res = convertor.convertDouble(rowBaseIterator.getDouble(columnIndex));
        break;
      }
      case Types.BINARY: {

      }
      case Types.VARBINARY: {

      }
      case Types.LONGVARBINARY: {
        res = convertor.convertBytes(rowBaseIterator.getBytes(columnIndex));
        break;
      }
      case Types.DATE: {
        res = convertor.convertDate(rowBaseIterator.getDate(columnIndex));
        break;
      }
      case Types.TIME: {
        res = convertor.convertTime(rowBaseIterator.getTime(columnIndex));
        break;
      }
      case Types.TIMESTAMP: {
        res = convertor.convertTimeStamp(rowBaseIterator.getTimestamp(columnIndex));
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

  }
}