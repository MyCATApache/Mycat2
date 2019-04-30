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

public interface ColumnDefPacket {
    static final byte[] DEFAULT_CATALOG = "def".getBytes();

    /**
     * buffer.skipInReading(4);
     * byte[] catalog = buffer.readLenencStringBytes();
     * byte[] schema = buffer.readLenencStringBytes();
     * byte[] table = buffer.readLenencStringBytes();
     * byte[] orgTable = buffer.readLenencStringBytes();
     * byte[] name = buffer.readLenencStringBytes();
     * byte[] orgName = buffer.readLenencStringBytes();
     * byte nextLength = buffer.readByte();
     * int charsetSet = (int) buffer.readFixInt(2);
     * int columnLength = (int) buffer.readFixInt(4);
     * byte type = (byte) (buffer.readByte() & 0xff);
     * int flags = (int) buffer.readFixInt(2);
     * byte decimals = buffer.readByte();
     * <p>
     * buffer.skipInReading(2);
     * if (buffer.packetReadStartIndex() != endPos) {
     * int i = buffer.readLenencInt();
     * byte[] defaultValues = buffer.readFixStringBytes(i);
     * }
     *
     * @return
     */
    public byte[] getColumnCatalog();

    public void setColumnCatalog(byte[] catalog);

    public byte[] getColumnSchema();

    public void setColumnSchema(byte[] schema);

    public byte[] getColumnTable();

    public void setColumnTable(byte[] table);

    public byte[] getColumnOrgTable();

    public void setColumnOrgTable(byte[] orgTable);

    public byte[] getColumnName();

    default public String getColumnNameString() {
        return new String(getColumnName());
    }

    public void setColumnName(byte[] name);

    public byte[] getColumnOrgName();

    public void setColumnOrgName(byte[] orgName);

    public int getColumnNextLength();

    public void setColumnNextLength(int nextLength);

    public int getColumnCharsetSet();

    public void setColumnCharsetSet(int charsetSet);

    public int getColumnLength();

    public void setColumnLength(int columnLength);

    public int getColumnType();

    public void setColumnType(int type);

    public int getColumnFlags();

    public void setColumnFlags(int flags);

    public byte getColumnDecimals();

    public void setColumnDecimals(byte decimals);

    public byte[] getColumnDefaultValues();

    public void setColumnDefaultValues(byte[] defaultValues);

    public String toString();

    public void writeToChannel(SocketChannel channel);

}
