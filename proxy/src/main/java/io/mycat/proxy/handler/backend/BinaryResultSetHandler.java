package io.mycat.proxy.handler.backend;

import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_BIT;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_BLOB;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_DATE;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_DATETIME;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_DECIMAL;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_DOUBLE;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_ENUM;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_FLOAT;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_GEOMETRY;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_LONG;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_LONGLONG;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_LONG_BLOB;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_MEDIUM_BLOB;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_NEWDATE;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_NEW_DECIMAL;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_NULL;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_SET;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_SHORT;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_STRING;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_TIME;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_TIMESTAMP;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_TINY;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_TINY_BLOB;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_VARCHAR;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_VAR_STRING;
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_YEAR;
import static io.mycat.beans.mysql.MySQLType.FIELD_TYPE_INT24;

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.collector.ResultSetTransfor;
import io.mycat.logTip.TaskTip;
import io.mycat.proxy.packet.ColumnDefPacketImpl;

/**
 * @author jamie12221
 *  date 2019-05-22 00:37
 **/
public class BinaryResultSetHandler implements ResultSetHandler {

  int binaryNullBitMapLength;
  int columnCount;
  ColumnDefPacket[] currentColumnDefList;
  ResultSetTransfor collector;

  /**
   * 预处理命令nullmap
   */
  private static void storeNullBitMap(byte[] nullBitMap, int i) {
    int bitMapPos = (i) / 8;
    int bitPos = (i) % 8;
    nullBitMap[bitMapPos] |= (byte) (1 << bitPos);
  }

  @Override
  public void onColumnCount(int columnCount) {
    this.columnCount = 0;
    this.binaryNullBitMapLength = (columnCount + 7 + 2) / 8;
    this.currentColumnDefList = new ColumnDefPacket[columnCount];
    collector.onResultSetStart();
  }

  @Override
  public void onOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.onResultSetStart();
    collector.onResultSetEnd();
  }

  @Override
  public void onRowEof(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.onResultSetEnd();
  }

  @Override
  public void onRowOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.onResultSetEnd();
  }

  @Override
  public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {
    ColumnDefPacketImpl packet = new ColumnDefPacketImpl();
    packet.read(mySQLPacket, startPos, endPos);
    int i = this.columnCount++;
    this.currentColumnDefList[i] = packet;
  }

  @Override
  public void onColumnDefEof(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.collectColumnList(currentColumnDefList);
  }

  @Override
  public void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.onRowStart();
    int nullBitMapStartPos = startPos + 4 + 1;
    int nullBitMapEndPos = nullBitMapStartPos + binaryNullBitMapLength;
    mySQLPacket.packetReadStartIndex(nullBitMapEndPos);
    for (int columnIndex = 0; columnIndex < currentColumnDefList.length; columnIndex++) {
      ColumnDefPacket columnDef = currentColumnDefList[columnIndex];
      int i = nullBitMapStartPos + (columnIndex + 2) / 8;
      byte aByte = mySQLPacket.getByte(i);
      boolean isNull = ((aByte & (1 << (columnIndex & 7))) != 0);
      int startIndex = mySQLPacket.packetReadStartIndex();
      int columnType = columnDef.getColumnType();
      if (isNull) {
        switch (columnType) {
          default: {
            throw new MycatExpection(TaskTip.UNKNOWN_FIELD_TYPE.getMessage(columnType));
          }
          case FIELD_TYPE_DECIMAL: {
            collector
                .collectNullDecimal(columnIndex, columnDef, columnDef.getColumnDecimals() & 0xff);
            break;
          }
          case FIELD_TYPE_TINY: {
            collector.collectNullTiny(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_SHORT: {
            collector.collectNullShort(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_LONG: {
            collector.collectNullLong(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_FLOAT: {
            collector.collectNullFloat(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_DOUBLE: {
            collector.collectNullDouble(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_NULL: {
            collector.collectNull(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_TIMESTAMP: {
            collector.collectNullTimestamp(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_LONGLONG: {
            collector.collectNullLongLong(columnIndex, columnDef);
            break;
          }
          case MySQLFieldsType.FIELD_TYPE_INT24: {
            collector.collectNullInt24(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_DATE: {
            collector.collectNullDate(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_TIME: {
            collector.collectNullTime(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_DATETIME: {
            collector.collectNullDatetime(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_YEAR: {
            collector.collectNullYear(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_NEWDATE: {
            collector.collectNullNewDate(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_VARCHAR: {
            collector.collectNullVarChar(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_BIT: {
            collector.collectNullBit(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_NEW_DECIMAL: {
            collector
                .collectNullNewDecimal(columnIndex, columnDef,
                    columnDef.getColumnDecimals() & 0xff);
            break;
          }
          case FIELD_TYPE_ENUM: {
            collector.collectNullEnum(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_SET: {
            collector.collectNullSet(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_TINY_BLOB: {
            collector.collectNullTinyBlob(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_MEDIUM_BLOB: {
            collector.collectNullMediumBlob(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_LONG_BLOB: {
            collector.collectNullLongBlob(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_BLOB: {
            collector.collectNullBlob(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_VAR_STRING: {
            collector.collectNullVarString(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_STRING: {
            collector.collectNullTinyString(columnIndex, columnDef);
            break;
          }
          case FIELD_TYPE_GEOMETRY: {
            collector.collectNullGeometry(columnIndex, columnDef);
            break;
          }
        }
        continue;
      }

      /**
       * 二进制格式,详细看协议,startIndex 是
       * 字符串类型,长度的开始位置;
       * 值类型,不带长度
       */
      switch (columnType) {
        default: {
          throw new MycatExpection(TaskTip.UNKNOWN_FIELD_TYPE.getMessage(columnType));
        }
        case FIELD_TYPE_DECIMAL: {
          collector.collectDecimal(columnIndex, columnDef, columnDef.getColumnDecimals() & 0xff,
              mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_TINY: {
          collector.collectTiny(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_SHORT: {
          collector.collectShort(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_LONG: {
          collector.collectLong(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_FLOAT: {
          collector.collectFloat(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_DOUBLE: {
          collector.collectDouble(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_NULL: {
          collector.collectNull(columnIndex, columnDef);
          break;
        }
        case FIELD_TYPE_TIMESTAMP: {
          collector.collectTimestamp(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_LONGLONG: {
          collector.collectLongLong(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_INT24: {
          collector.collectInt24(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_DATE: {
          collector.collectDate(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_TIME: {
          collector.collectTime(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_DATETIME: {
          collector.collectDatetime(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_YEAR: {
          collector.collectYear(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_NEWDATE: {
          collector.collectNewDate(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_VARCHAR: {
          collector.collectVarChar(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_BIT: {
          collector.collectBit(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_NEW_DECIMAL: {
          collector.collectNewDecimal(columnIndex, columnDef, columnDef.getColumnDecimals() & 0xff,
              mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_ENUM: {
          collector.collectEnum(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_SET: {
          collector.collectSet(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_TINY_BLOB: {
          collector.collectTinyBlob(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_MEDIUM_BLOB: {
          collector.collectMediumBlob(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_LONG_BLOB: {
          collector.collectLongBlob(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_BLOB: {
          collector.collectBlob(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_VAR_STRING: {
          collector.collectVarString(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_STRING: {
          collector.collectTinyString(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
        case FIELD_TYPE_GEOMETRY: {
          collector.collectGeometry(columnIndex, columnDef, mySQLPacket, startIndex);
          break;
        }
      }

    }
    collector.onRowEnd();
  }
}
