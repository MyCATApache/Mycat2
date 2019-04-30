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
package io.mycat.proxy.packet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

public enum  ResultSetCollectorImpl implements ResultSetCollector {
    INSTANCE;
    static final Logger logger = LoggerFactory.getLogger(ResultSetCollectorImpl.class);
    @Override
    public void onResultSetStart() {
        logger.debug("onResultSetStart");
    }

    @Override
    public void onResultSetEnd() {
        logger.debug("onResultSetEnd");
    }

    @Override
    public void collectColumnList(ColumnDefPacket[] packets) {
        logger.debug("collectColumnList");
        logger.debug(Arrays.toString(packets));
    }

    @Override
    public void onRowStart() {
        logger.debug("onRowStart");
    }

    @Override
    public void onRowEnd() {
        logger.debug("onRowEnd");
    }

    @Override
    public void collectDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale, MySQLPacket mySQLPacket, int startIndex) {
        byte[] bytes = mySQLPacket.getLenencBytes(startIndex);
        BigDecimal bigDecimal = new BigDecimal(new BigInteger(bytes), decimalScale);
        logger.debug("{}:{}",columnDef.getColumnNameString(),bigDecimal);
    }

    @Override
    public void collectTiny(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getByte(startIndex));
    }

    @Override
    public void collectGeometry(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencBytes(startIndex));
    }

    @Override
    public void collectTinyString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectVarString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectShort(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencInt(startIndex));
    }

    @Override
    public void collectBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencBytes(startIndex));
    }

    @Override
    public void collectMediumBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencBytes(startIndex));
    }

    @Override
    public void collectTinyBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencBytes(startIndex));
    }

    @Override
    public void collectFloat(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),Double.longBitsToDouble(mySQLPacket.getLenencInt(startIndex)));
    }

    @Override
    public void collectDouble(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),Double.longBitsToDouble(mySQLPacket.getLenencInt(startIndex)));
    }

    @Override
    public void collectNull(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),null);
    }

    @Override
    public void collectTimestamp(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),null);
    }

    @Override
    public void collectInt24(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencInt(startIndex));
    }

    @Override
    public void collectDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectTime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectDatetime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectYear(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencInt(startIndex));
    }

    @Override
    public void collectNewDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectVarChar(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectBit(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencInt(startIndex));
    }

    @Override
    public void collectNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectEnum(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectSet(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencString(startIndex));
    }

    @Override
    public void collectLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencInt(startIndex));
    }

    @Override
    public void collectLongLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencInt(startIndex));
    }

    @Override
    public void collectLongBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket, int startIndex) {
        logger.debug("{}:{}",columnDef.getColumnNameString(),mySQLPacket.getLenencBytes(startIndex));
    }


}
