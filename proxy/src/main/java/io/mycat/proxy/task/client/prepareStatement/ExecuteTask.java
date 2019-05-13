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
package io.mycat.proxy.task.client.prepareStatement;

import static io.mycat.beans.mysql.MySQLFieldsType.BINARY_FLAG;
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

import io.mycat.MycatExpection;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLPStmtBindValueList;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLPrepareStmtExecuteFlag;
import io.mycat.beans.mysql.MySQLPreparedStatement;
import io.mycat.logTip.TaskTip;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.buffer.ProxyBufferImpl;
import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.packet.ColumnDefPacketImpl;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.packet.ResultSetCollector;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsyncTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;
import java.util.Objects;

/**
 * @author jamie12221 chen junwen
 * @date 2019-05-10 21:13 预处理执行类 该任务类具备收集二进制结果集
 **/
public class ExecuteTask implements ResultSetTask {

  int binaryNullBitMapLength;
  int columnCount;
  ColumnDefPacket[] currentColumnDefList;
  ResultSetCollector collector;

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
      if (isNull) {
        collector
            .collectNull(columnIndex, columnDef, mySQLPacket, mySQLPacket.packetReadStartIndex());
        continue;
      }
      int columnType = columnDef.getColumnType();
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
          collector.collectNull(columnIndex, columnDef, mySQLPacket, startIndex);
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

  /**
   *
   */
  public void request(MySQLClientSession mysql, MySQLPreparedStatement ps,
      MySQLPrepareStmtExecuteFlag flags, ResultSetCollector collector,
      AsyncTaskCallBack<MySQLClientSession> callBack) {
    Objects.requireNonNull(ps);
    Objects.requireNonNull(ps.getLongDataMap());
    Objects.requireNonNull(collector);
    Objects.requireNonNull(callBack);
    Objects.requireNonNull(flags);
    assert (mysql.currentProxyBuffer() == null);
    if (!ps.getLongDataMap().isEmpty()) {
      new SendLongDataTask()
          .request(mysql, ps, (session, sender, success, result, errorMessage) -> {
            request(mysql, ps, flags, collector, callBack);
          });
      return;
    }
    this.collector = collector;
    try {
      final long iteration = 1;
      assert (mysql.currentProxyBuffer() == null);
      mysql.setCallBack(callBack);
      mysql.switchNioHandler(this);
      MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
      mysql.setCurrentProxyBuffer(new ProxyBufferImpl(thread.getBufPool()));
      try (MySQLPayloadWriter writer = new MySQLPayloadWriter()) {
        writer.writeByte((byte) 0x17);
        writer.writeFixInt(4, ps.getStatementId());
        writer.writeByte(flags.getValue());
        writer.writeFixInt(4, iteration);
        int parametersNumber = ps.getParametersNumber();
        if (parametersNumber > 0) {
          MySQLPStmtBindValueList mySQLPStmtBindValueList = ps.getBindValueList();
          Object[] valueList = mySQLPStmtBindValueList.getValueList();
          byte[] nullBitMap = mySQLPStmtBindValueList.getCacheNullBitMap();
          for (int i = 0; i < valueList.length; i++) {
            if (valueList[i] == null) {
              storeNullBitMap(nullBitMap, i);
            }
          }
          writer.writeBytes(nullBitMap);
          writer.writeByte((byte) (ps.isNewParameterBoundFlag() ? 1 : 0));
          int[] parameterTypeList = mySQLPStmtBindValueList.getParameterTypeList();
          if (ps.isNewParameterBoundFlag()) {
            for (int j = 0; j < parameterTypeList.length; j++) {
              writer.writeByte((byte) parameterTypeList[j]);
              writer.writeByte((byte) BINARY_FLAG);
            }
          }
          for (int j = 0; j < valueList.length; j++) {
            Object o = valueList[j];
            if (valueList[j] != null) {
              switch (parameterTypeList[j]) {
                case MySQLFieldsType.FIELD_TYPE_TINY:
                  writer.writeByte((Byte) o);
                  break;
                case MySQLFieldsType.FIELD_TYPE_SHORT:
                  writer.writeShort((Short) o);
                  break;
                case MySQLFieldsType.FIELD_TYPE_LONG:
                  writer.writeLong((Long) o);
                  break;
                case MySQLFieldsType.FIELD_TYPE_FLOAT:
                  writer.writeFloat((Float) o);
                  break;
                case MySQLFieldsType.FIELD_TYPE_DOUBLE:
                  writer.writeDouble((Double) o);
                  break;
                case MySQLFieldsType.FIELD_TYPE_BLOB:
                  break;
                case MySQLFieldsType.FIELD_TYPE_STRING:
                  writer.writeEOFString((String) o);
                  break;
                default:
                  throw new MycatExpection("todo support date");//@todo support date
              }
            }
          }
        }
        mysql.prepareReveiceResponse();
        MySQLPacket mySQLPacket = mysql.newCurrentProxyPacket(writer.size());
        mySQLPacket.writeBytes(writer.toByteArray());
        mysql.writeCurrentProxyPacket(mySQLPacket, 0);
      }
    } catch (Exception e) {
      this.clearAndFinished(mysql, false, mysql.setLastMessage(e));
    }
  }
}
