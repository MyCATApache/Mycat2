/**
 * Copyright (C) <2020>  <chen junwen>
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


import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.MySQLPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * 文本结果集收集类
 *
 * @author jamie12221
 *  date 2019-05-10 13:21
 */

/**
 *
 * chen junwen
 *
 * a proxy packet collector,collecting mysql binary row
 */
public class BinaryResultSetTransforCollector implements ResultSetTransfor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryResultSetTransforCollector.class);

    final ResultSetCollector collector;

    public BinaryResultSetTransforCollector(
            ResultSetCollector collector) {
        this.collector = collector;
    }

    @Override
    public void onResultSetStart() {
        collector.onResultSetStart();
    }

    @Override
    public void onResultSetEnd() {
        collector.onResultSetEnd();
    }

    @Override
    public void onRowStart() {
        collector.onRowStart();
    }

    @Override
    public void onRowEnd() {
        collector.onRowEnd();
    }

    @Override
    public void collectDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale,
                               MySQLPacket mySQLPacket, int startIndex) {
        BigDecimal bigDecimal = new BigDecimal(mySQLPacket.readLenencString());
        collector.addDecimal(columnIndex, bigDecimal);
    }

    @Override
    public void collectTiny(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        int i = mySQLPacket.readByte() & 0xff;
        collector.addValue(columnIndex, i, false);
    }

    @Override
    public void collectGeometry(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                int startIndex) {
        String lenencBytes = mySQLPacket.readLenencString();
        collector.addString(columnIndex, lenencBytes);
    }

    @Override
    public void collectTinyString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                  int startIndex) {
        String lenencBytes = mySQLPacket.readLenencString();
        collector.addString(columnIndex, lenencBytes);
    }

    @Override
    public void collectVarString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                 int startIndex) {
        String lenencBytes = mySQLPacket.readLenencString();
        collector.addString(columnIndex, lenencBytes);
    }

    @Override
    public void collectShort(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                             int startIndex) {
        int lenencInt = (int) mySQLPacket.readFixInt(2);
        collector.addValue(columnIndex, lenencInt, false);
    }

    @Override
    public void collectBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        byte[] lenencBytes = mySQLPacket.readLenencBytes();
        collector.addBlob(columnIndex, lenencBytes);
    }

    @Override
    public void collectMediumBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                  int startIndex) {
        byte[] lenencBytes = mySQLPacket.readLenencBytes();
        collector.addBlob(columnIndex, lenencBytes);
    }

    @Override
    public void collectTinyBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                int startIndex) {
        byte[] lenencBytes = mySQLPacket.readLenencBytes();
        collector.addBlob(columnIndex, lenencBytes);
    }

    @Override
    public void collectFloat(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                             int startIndex) {
        float v = mySQLPacket.readFloat();
        collector.addValue(columnIndex, v, false);
    }

    @Override
    public void collectDouble(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                              int startIndex) {
        double v = mySQLPacket.readDouble();
        collector.addValue(columnIndex, v, false);
    }

    @Override
    public void collectNull(int columnIndex, ColumnDefPacket columnDef) {
        collector.addNull(columnIndex);
    }

    @Override
    public void collectTimestamp(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                 int startIndex) {
        java.util.Date date = mySQLPacket.readDate();
        collector.addDate(columnIndex, date);
    }


    @Override
    public void collectInt24(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                             int startIndex) {
        int lenencInt = (int) mySQLPacket.readFixInt(4);
        collector.addValue(columnIndex, lenencInt, false);
    }

    @Override
    public void collectDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        java.util.Date date = mySQLPacket.readDate();
        collector.addDate(columnIndex, date);
        LOGGER.debug("{}:{}", columnDef.getColumnNameString(), date);
    }

    @Override
    public void collectTime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        java.util.Date date = mySQLPacket.readDate();
        collector.addDate(columnIndex, date);
    }

    @Override
    public void collectDatetime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                int startIndex) {
        java.util.Date date = mySQLPacket.readDate();
        collector.addDate(columnIndex, date);
    }

    @Override
    public void collectYear(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        java.util.Date date = mySQLPacket.readDate();
        collector.addDate(columnIndex, date);
    }

    @Override
    public void collectNewDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                               int startIndex) {
        java.util.Date date = mySQLPacket.readDate();
        collector.addDate(columnIndex, date);
    }

    @Override
    public void collectVarChar(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                               int startIndex) {
        if (true) {
            String lenencString = mySQLPacket.readLenencString();
            collector.addString(columnIndex, lenencString);
            LOGGER.debug("{}:{}", columnDef.getColumnNameString(), lenencString);
        }
    }

    @Override
    public void collectBit(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                           int startIndex) {
        String lenencBytes = mySQLPacket.readLenencString();
        collector.addString(columnIndex, lenencBytes);
    }

    @Override
    public void collectNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale,
                                  MySQLPacket mySQLPacket, int startIndex) {
        BigDecimal bigDecimal = mySQLPacket.readBigDecimal();
        collector.addDecimal(columnIndex, bigDecimal);
    }

    @Override
    public void collectEnum(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        String lenencBytes = mySQLPacket.readLenencString();
        collector.addString(columnIndex, lenencBytes);
    }

    @Override
    public void collectSet(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                           int startIndex) {
        String lenencBytes = mySQLPacket.readLenencString();
        collector.addString(columnIndex, lenencBytes);
    }

    @Override
    public void collectLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                            int startIndex) {
        long lenencInt = mySQLPacket.readLong();
        collector.addValue(columnIndex, lenencInt, false);
    }

    @Override
    public void collectLongLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                int startIndex) {
        long lenencInt = mySQLPacket.readLong();
        collector.addValue(columnIndex, lenencInt, false);
    }

    @Override
    public void collectLongBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
                                int startIndex) {
        byte[] lenencBytes = mySQLPacket.readLenencBytes();
        collector.addBlob(columnIndex, lenencBytes);
    }

    @Override
    public void collectNullDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale) {
        collector.addDecimal(columnIndex, null);
    }

    @Override
    public void collectNullTiny(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0, true);
    }

    @Override
    public void collectNullGeometry(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullTinyString(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullVarString(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullShort(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0, true);
    }

    @Override
    public void collectNullBlob(int columnIndex, ColumnDefPacket columnDef) {
        collector.addBlob(columnIndex, null);
    }

    @Override
    public void collectNullMediumBlob(int columnIndex, ColumnDefPacket columnDef) {
        collector.addBlob(columnIndex, null);
    }

    @Override
    public void collectNullTinyBlob(int columnIndex, ColumnDefPacket columnDef) {
        collector.addBlob(columnIndex, null);
    }

    @Override
    public void collectNullFloat(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0.0, true);
    }

    @Override
    public void collectNullDouble(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0.0, true);
    }

    @Override
    public void collectNullTimestamp(int columnIndex, ColumnDefPacket columnDef) {
        collector.addDate(columnIndex, null);
    }

    @Override
    public void collectNullInt24(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0, true);
    }

    @Override
    public void collectNullDate(int columnIndex, ColumnDefPacket columnDef) {
        collector.addDate(columnIndex, null);
    }

    @Override
    public void collectNullTime(int columnIndex, ColumnDefPacket columnDef) {
        collector.addDate(columnIndex, null);
    }

    @Override
    public void collectNullDatetime(int columnIndex, ColumnDefPacket columnDef) {
        collector.addDate(columnIndex, null);
    }

    @Override
    public void collectNullYear(int columnIndex, ColumnDefPacket columnDef) {
        collector.addDate(columnIndex, null);
    }

    @Override
    public void collectNullNewDate(int columnIndex, ColumnDefPacket columnDef) {
        collector.addDate(columnIndex, null);
    }

    @Override
    public void collectNullVarChar(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullBit(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale) {
        collector.addDecimal(columnIndex, null);
    }

    @Override
    public void collectNullEnum(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullSet(int columnIndex, ColumnDefPacket columnDef) {
        collector.addString(columnIndex, null);
    }

    @Override
    public void collectNullLong(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0, true);
    }

    @Override
    public void collectNullLongLong(int columnIndex, ColumnDefPacket columnDef) {
        collector.addValue(columnIndex, 0, true);
    }

    @Override
    public void collectNullLongBlob(int columnIndex, ColumnDefPacket columnDef) {
        collector.addBlob(columnIndex, null);
    }

    @Override
    public void collectColumnList(ColumnDefPacket[] packets) {
        collector.collectColumnList(packets);
    }
}
