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

import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class ColumnDefPacketImpl implements ColumnDefPacket {
    //    byte[] columnCatalog;
    byte[] columnSchema;
    byte[] columnTable;
    byte[] columnOrgTable;
    byte[] columnName;
    byte[] columnOrgName;
    int columnNextLength;
    int columnCharsetSet;
    int columnLength;

    @Override
    public String toString() {
        return "ColumnDefPacketImpl{" +
                "columnCatalog=" + new String(ColumnDefPacket.DEFAULT_CATALOG) +
                ", columnSchema=" + new String(columnSchema) +
                ", columnTable=" + new String(columnTable) +
                ", columnOrgTable=" + new String(columnOrgTable) +
                ", columnName=" + new String(columnName) +
                ", columnOrgName=" + new String(columnOrgName) +
                ", columnNextLength=" + columnNextLength +
                ", columnCharsetSet=" + columnCharsetSet +
                ", columnLength=" + columnLength +
                ", columnType=" + columnType +
                ", columnFlags=" + columnFlags +
                ", columnDecimals=" + columnDecimals +
                ", columnDefaultValues=" + Arrays.toString(columnDefaultValues) +
                '}';
    }

    @Override
    public void writeToChannel(SocketChannel channel) {

    }

    byte columnType;
    int columnFlags;
    byte columnDecimals;
    byte[] columnDefaultValues;

    @Override
    public byte[] getColumnCatalog() {
        return ColumnDefPacket.DEFAULT_CATALOG;
    }

    @Override
    public void setColumnCatalog(byte[] catalog) {

    }

    @Override
    public byte[] getColumnSchema() {
        return columnSchema;
    }

    @Override
    public void setColumnSchema(byte[] schema) {
        this.columnSchema = schema;
    }

    @Override
    public byte[] getColumnTable() {
        return columnTable;
    }

    @Override
    public void setColumnTable(byte[] table) {
        this.columnTable = table;
    }

    @Override
    public byte[] getColumnOrgTable() {
        return columnOrgTable;
    }

    @Override
    public void setColumnOrgTable(byte[] orgTable) {
        this.columnOrgTable = orgTable;
    }

    @Override
    public byte[] getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnName(byte[] name) {
        this.columnName = name;
    }

    @Override
    public byte[] getColumnOrgName() {
        return columnOrgName;
    }

    @Override
    public void setColumnOrgName(byte[] orgName) {
        this.columnOrgName = orgName;
    }

    @Override
    public int getColumnNextLength() {
        return columnNextLength;
    }

    @Override
    public void setColumnNextLength(int nextLength) {
        this.columnLength = nextLength;
    }

    @Override
    public int getColumnCharsetSet() {
        return columnCharsetSet;
    }

    @Override
    public void setColumnCharsetSet(int charsetSet) {
        this.columnCharsetSet = charsetSet;
    }

    @Override
    public int getColumnLength() {
        return columnLength;
    }

    @Override
    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    @Override
    public int getColumnType() {
        return columnType & 0xff;
    }

    @Override
    public void setColumnType(int type) {
        this.columnType = (byte) type;
    }

    @Override
    public int getColumnFlags() {
        return columnFlags;
    }

    @Override
    public void setColumnFlags(int flags) {
        this.columnFlags = flags;
    }

    @Override
    public byte getColumnDecimals() {
        return this.columnDecimals;
    }

    @Override
    public void setColumnDecimals(byte decimals) {
        this.columnDecimals = decimals;
    }

    @Override
    public byte[] getColumnDefaultValues() {
        return columnDefaultValues;
    }

    @Override
    public void setColumnDefaultValues(byte[] defaultValues) {
        this.columnDefaultValues = defaultValues;
    }

    public void read(MySQLPacket buffer, int startPos, int endPos) {
        buffer.skipInReading(4);
        buffer.readLenencStringBytes();
        this.columnSchema = buffer.readLenencStringBytes();
        this.columnTable = buffer.readLenencStringBytes();
        this.columnOrgTable = buffer.readLenencStringBytes();
        this.columnName = buffer.readLenencStringBytes();
        this.columnOrgName = buffer.readLenencStringBytes();
        this.columnNextLength = buffer.readByte();
        this.columnCharsetSet = (int) buffer.readFixInt(2);
        this.columnLength = (int) buffer.readFixInt(4);
        this.columnType = (byte) (buffer.readByte() & 0xff);
        this.columnFlags = (int) buffer.readFixInt(2);
        this.columnDecimals = buffer.readByte();
        buffer.skipInReading(2);
        if (buffer.packetReadStartIndex() != endPos) {
            int i = buffer.readLenencInt();
            this.columnDefaultValues = buffer.readFixStringBytes(i);
        }
    }

}
