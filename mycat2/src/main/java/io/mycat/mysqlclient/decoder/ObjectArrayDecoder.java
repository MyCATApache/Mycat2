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
import io.mycat.mysqlclient.PacketUtil;
import io.mycat.vertx.ReadView;
import io.vertx.core.buffer.Buffer;
import io.vertx.mysqlclient.impl.datatype.DataType;
import lombok.Getter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

@Getter
public class ObjectArrayDecoder implements Decoder<Object[]> {
    protected int columnCount;
    protected ColumnDefPacket[] columnDefPackets;
    protected DataType[] dataTypes;
    protected boolean[] signeds;
    int pos = 0;

    @Override
    public void initColumnCount(int count) {
        this.columnCount = count;
        this.columnDefPackets = new ColumnDefPacketImpl[count];
        this.dataTypes = new DataType[count];
        this.signeds = new boolean[count];
    }

    @Override
    public void addColumn(int index, Buffer buffer) {
        ColumnDefPacketImpl columnDefPacket = decodeColumnDefinitionPacketPayload(buffer);
        this.columnDefPackets[index] = columnDefPacket;
        this.dataTypes[index] = DataType.valueOf(columnDefPacket.getColumnType());
        this.signeds[index] = columnDefPacket.isSigned();
    }

    public long readLengthEncodedInteger(Buffer buffer) {
        short firstByte = buffer.getUnsignedByte(pos);

        switch (firstByte) {
            case 0xFB:
                pos += 1;
                return -1;
            case 0xFC:
                pos += 1;
                int unsignedShortLE = buffer.getUnsignedShortLE(pos);//2
                pos += 2;
                return unsignedShortLE;
            case 0xFD:
                pos += 1;
                int unsignedMediumLE = buffer.getUnsignedMediumLE(pos);//3
                pos += 3;
                return unsignedMediumLE;
            case 0xFE:
                pos += 1;
                long longLE = buffer.getLongLE(pos);//8
                pos += 8;
                return longLE;
            default:
                pos += 1;
                return firstByte;
        }
    }

    @Override
    public Object[] convert(Buffer payload) {
        try {
            pos = 0;
            final int NULL = 0xFB;
            Object[] row = new Object[columnCount];

            // TEXT row decoding

            for (int c = 0; c < columnCount; c++) {
                if ((payload.getByte(pos) & 0xff) == NULL) {
                    pos++;
                } else {
                    int startPos = pos;
                    int length = (int) readLengthEncodedInteger(payload);
                    Object value = null;
                    DataType dataType = this.dataTypes[c];
                    switch (dataType) {
                        case INT1:
                        case INT2:
                        case INT3:
                        case INT4:
                        case INT8: {
                            boolean signed = this.signeds[c];
                            long l;
                            switch (dataType) {
                                case INT1: {
                                    l = PacketUtil.decodeDecStringToLong(pos, length, payload);
                                    if (signed) {
                                        value = (byte) l;
                                    } else {
                                        value = (short) l;
                                    }
                                    break;
                                }
                                case INT2: {
                                    l = PacketUtil.decodeDecStringToLong(pos, length, payload);
                                    if (signed) {
                                        value = (short) l;
                                    } else {
                                        value = (int) l;
                                    }
                                    break;
                                }
                                case INT3: {
                                    l = PacketUtil.decodeDecStringToLong(pos, length, payload);
                                    value = (int) l;
                                    break;
                                }
                                case INT4: {
                                    l = PacketUtil.decodeDecStringToLong(pos, length, payload);
                                    if (signed) {
                                        value = (int) l;
                                    } else {
                                        value = (long) l;
                                    }
                                    break;
                                }
                                case INT8: {
                                    if (signed) {
                                        value = PacketUtil.decodeDecStringToLong(pos, length, payload);
                                        break;
                                    } else {
                                        String string = payload.getString(pos, pos + length);
                                        value = new BigInteger(string);
                                    }
                                    break;
                                }
                            }
                            break;
                        }
                        case DOUBLE: {
                            String string = payload.getString(pos, pos + length);
                            value = Double.parseDouble(string);
                            break;
                        }
                        case FLOAT: {
                            String string = payload.getString(pos, pos + length);
                            value = Float.parseFloat(string);
                            break;
                        }
                        case NUMERIC: {
                            String string = payload.getString(pos, pos + length);
                            value = new BigDecimal(string);
                            break;
                        }
                        case JSON:
                        case GEOMETRY:
                        case STRING:
                        case VARSTRING:
                            if (columnDefPackets[c].getColumnCharsetSet() == 63) {
                                value = payload.getBytes(pos, pos + length);
                            } else {
                                value = payload.getString(pos, pos + length);
                            }
                            break;
                        case TINYBLOB:
                        case BLOB:
                        case MEDIUMBLOB:
                        case LONGBLOB:
                            value = payload.getBytes(pos, pos + length);
                            break;
                        case DATE: {
                            try {
                                String string = payload.getString(pos, pos + length);
                                if (string.equals("0000-00-00")) {
                                    value = null;
                                } else {
                                    value = LocalDate.parse(string);
                                }
                            } catch (Exception e) {
                                System.out.println();
                            }
                            break;
                        }
                        case TIME: {
                            value = PacketUtil.textDecodeTime(payload.getString(pos, pos + length));
                            break;
                        }
                        case TIMESTAMP:
                        case DATETIME:
                            String cs = payload.getString(pos, pos + length);
                            value = PacketUtil.textDecodeDateTime(cs);
                            break;
                        case YEAR:
                            value = PacketUtil.textDecodeDateTime(payload.getString(pos, pos + length));
                            break;
                        case BIT:
                            value = PacketUtil.decodeBit(payload, pos, pos + length);
                            break;
                        case NULL:
                            value = null;
                            break;
                        case UNBIND:
                            throw new UnsupportedOperationException();
                    }
                    pos = pos + length;
                    row[c] = value;
                }
            }
            return row;
        } finally {

        }
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
