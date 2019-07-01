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
import static io.mycat.beans.mysql.MySQLFieldsType.FIELD_TYPE_INT24;
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

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.collector.ResultSetTransfor;
import io.mycat.proxy.packet.ColumnDefPacketImpl;
import java.util.function.IntPredicate;

/**
 * @author jamie12221
 *  date 2019-05-10 21:57 文本结果集任务
 *
 * 可以根据字段的下标跳过指定列的数据解析
 **/
public class TextResultSetHandler implements ResultSetHandler {
  int columnCount;
  ColumnDefPacket[] currentColumnDefList;
  ResultSetTransfor collector;
  IntPredicate predicate;

  public TextResultSetHandler(ResultSetTransfor collector) {
    this.collector = collector;
    this.predicate = (i) -> true;
  }

  public TextResultSetHandler(ResultSetTransfor collector, IntPredicate predicate) {
    this.collector = collector;
    this.predicate = predicate;
  }
//  public void request(
//      MySQLClientSession mysql, String sql, IntPredicate predicate, ResultSetTransfor collector,
//      ResultSetCallBack<MySQLClientSession> callBack) {
//    this.collector = collector;
//    this.predicate = predicate;
//    request(mysql, 3, sql, callBack);
//  }

  /**
   * 字段过滤器
   */
  protected boolean columnFilter(int columnIndex) {
    return predicate.test(columnIndex);
  }

//  public void request(
//      MySQLClientSession mysql, String sql,
//      ResultSetCallBack<MySQLClientSession> callBack) {
//    this.collector = new ResultSetCollector();
//    this.predicate = (i) -> true;
//    request(mysql, 3, sql, callBack);
//  }

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
  public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) throws MycatException {
    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
      int startIndex = mySQLPacket.packetReadStartIndex();
      if (!columnFilter(columnIndex)) {
        mySQLPacket.skipLenencBytes(startIndex);
        continue;
      }
      ColumnDefPacket columnDef = currentColumnDefList[columnIndex];
      int columnType = columnDef.getColumnType();
      if (isNull(mySQLPacket, startIndex)) {
        mySQLPacket.packetReadStartIndex(startIndex + 1);
        switch (columnType) {
          default: {
            throw new MycatException("unknown field type:{}", (columnType));
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
          case FIELD_TYPE_INT24: {
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
      switch (columnType) {
        default: {
          throw new MycatException("unknown field type:{}", (columnType));
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
      int mayBeErrorStartIndex = mySQLPacket.packetReadStartIndex();
      if (mySQLPacket.skipLenencBytes(startIndex) == mayBeErrorStartIndex) {
      }
    }
    collector.onRowEnd();
  }

  boolean isNull(MySQLPacket packet, int startIndex) {
    return (packet.getByte(startIndex) & 0xff) == 0xfb;
  }

  @Override
  public void onColumnCount(int columnCount) {
    this.columnCount = 0;
    this.currentColumnDefList = new ColumnDefPacket[columnCount];
    collector.onResultSetStart();
  }

  @Override
  public void onRowEof(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.onResultSetEnd();
  }

  @Override
  public void onRowOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
    collector.onResultSetEnd();
  }

}
