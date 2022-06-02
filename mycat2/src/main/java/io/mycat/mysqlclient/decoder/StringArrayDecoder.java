/**
 * Copyright (C) <2022>  <chen junwen>
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

package io.mycat.mysqlclient.decoder;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ColumnDefPacketImpl;
import io.mycat.mysqlclient.Decoder;
import io.mycat.vertx.ReadView;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;

@Getter
public class StringArrayDecoder implements Decoder<Object[]> {
    int columnCount;
    ColumnDefPacket[] columnDefPackets;

    @Override
    public void initColumnCount(int count) {
        this.columnCount = count;
        this.columnDefPackets = new ColumnDefPacketImpl[count];
    }

    @Override
    public void addColumn(int index, Buffer buffer) {
        columnDefPackets[index] = decodeColumnDefinitionPacketPayload(buffer);
    }

    @Override
    public Object[] convert(Buffer payload) {
        final int NULL = 0xFB;
        Object[] row = new Object[columnCount];
        ReadView readView = new ReadView(payload);
        // TEXT row decoding
        for (int c = 0; c < columnCount; c++) {
            if ((readView.getByte() & 0xff) == NULL) {
                readView.skipInReading(1);
            } else {
                ColumnDefPacket columnDefPacket = columnDefPackets[c];
                int columnFlags = columnDefPacket.getColumnFlags();
                int columnType = columnDefPacket.getColumnType();
                if (columnDefPacket.getColumnCharsetSet() == 63){
                    row[c]= readView.readLenencBytes();
                }else {
                    row[c]= readView.readLenencString();
                }
            }
        }
        return row;
    }

    @Override
    public void onColumnEnd() {

    }

    ColumnDefPacketImpl decodeColumnDefinitionPacketPayload(Buffer payload) {
        ReadView readView = new ReadView(payload);
        byte[] catalog = readView.readLenencStringBytes();
        byte[] schema = readView.readLenencStringBytes();
        byte[] table = readView.readLenencStringBytes();
        byte[] orgTable = readView.readLenencStringBytes();
        byte[] name = readView.readLenencStringBytes();
        byte[] orgName = readView.readLenencStringBytes();
        long lengthOfFixedLengthFields = readView.readLenencInt();
        int characterSet = (int) readView.readFixInt(2);
        long columnLength = (int) readView.readFixInt(4);
        int type = readView.readByte() & 0xff;
        int flags = (int) readView.readFixInt(2);
        byte decimals = readView.readByte();

        ColumnDefPacketImpl columnDefPacket = new ColumnDefPacketImpl();
        columnDefPacket.setColumnSchema(schema);
        columnDefPacket.setColumnTable(table);
        columnDefPacket.setColumnOrgTable(orgTable);
        columnDefPacket.setColumnName(name);
        columnDefPacket.setColumnOrgName(orgName);
        columnDefPacket.setColumnCharsetSet(characterSet);
        columnDefPacket.setColumnLength((int) columnLength);
        columnDefPacket.setColumnType(type);
        columnDefPacket.setColumnFlags(flags);
        columnDefPacket.setColumnDecimals(decimals);

        return columnDefPacket;
    }

}
