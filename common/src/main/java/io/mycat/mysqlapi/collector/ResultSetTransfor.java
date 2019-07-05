/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.mysqlapi.collector;

import io.mycat.annotations.NoExcept;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.MySQLPacket;

/**
 * 结果集收集接口 int startIndex的含义应该是根据实际情况而定的
 *
 * @author jamie12221
 *  date 2019-05-10 13:21
 */
@NoExcept
public interface ResultSetTransfor {

    void onResultSetStart();

    void onResultSetEnd();

    void collectColumnList(ColumnDefPacket[] packets);

    void onRowStart();

    void onRowEnd();

    void collectDecimal(int columnIndex,ColumnDefPacket columnDef,int decimalScale, MySQLPacket mySQLPacket, int startIndex);

    void collectTiny(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectGeometry(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTinyString(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectVarString(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectShort(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


    void collectBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


    void collectMediumBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTinyBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectFloat(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectDouble(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

  void collectNull(int columnIndex, ColumnDefPacket columnDef);

    void collectTimestamp(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


    void collectInt24(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectDate(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectTime(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectDatetime(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectYear(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectNewDate(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectVarChar(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectBit(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectNewDecimal(int columnIndex,ColumnDefPacket columnDef,int decimalScale, MySQLPacket mySQLPacket, int startIndex);

    void collectEnum(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectSet(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectLong(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectLongLong(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);

    void collectLongBlob(int columnIndex,ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex);


  void collectNullDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale);

  void collectNullTiny(int columnIndex, ColumnDefPacket columnDef);

  void collectNullGeometry(int columnIndex, ColumnDefPacket columnDef);

  void collectNullTinyString(int columnIndex, ColumnDefPacket columnDef);

  void collectNullVarString(int columnIndex, ColumnDefPacket columnDef);

  void collectNullShort(int columnIndex, ColumnDefPacket columnDef);


  void collectNullBlob(int columnIndex, ColumnDefPacket columnDef);


  void collectNullMediumBlob(int columnIndex, ColumnDefPacket columnDef);

  void collectNullTinyBlob(int columnIndex, ColumnDefPacket columnDef);

  void collectNullFloat(int columnIndex, ColumnDefPacket columnDef);

  void collectNullDouble(int columnIndex, ColumnDefPacket columnDef);


  void collectNullTimestamp(int columnIndex, ColumnDefPacket columnDef);


  void collectNullInt24(int columnIndex, ColumnDefPacket columnDef);

  void collectNullDate(int columnIndex, ColumnDefPacket columnDef);

  void collectNullTime(int columnIndex, ColumnDefPacket columnDef);

  void collectNullDatetime(int columnIndex, ColumnDefPacket columnDef);

  void collectNullYear(int columnIndex, ColumnDefPacket columnDef);

  void collectNullNewDate(int columnIndex, ColumnDefPacket columnDef);

  void collectNullVarChar(int columnIndex, ColumnDefPacket columnDef);

  void collectNullBit(int columnIndex, ColumnDefPacket columnDef);

  void collectNullNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale);

  void collectNullEnum(int columnIndex, ColumnDefPacket columnDef);

  void collectNullSet(int columnIndex, ColumnDefPacket columnDef);

  void collectNullLong(int columnIndex, ColumnDefPacket columnDef);

  void collectNullLongLong(int columnIndex, ColumnDefPacket columnDef);

  void collectNullLongBlob(int columnIndex, ColumnDefPacket columnDef);
}
