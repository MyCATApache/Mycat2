package io.mycat.mysql.packet;

import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.BufferUtil;

/**
 * Created by linxiaofang on 2018/11/13.
 */
public class ColumnDefinitionPacket extends MySQLPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte NEXT_LENGTH = 0x0c;
    private static final byte[] FILLER = { 00, 00 };

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] schema;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
    public byte nextLength;
    public int charsetSet;
    public long columnLength;
    public byte type;
    public int flags;
    public byte decimals;
    public int filler;
    public byte[] defaultValues;

    public void readPayload(ProxyBuffer buffer) {
        // packet length:3
        packetLength = (int)buffer.readFixInt(3);
        // packet number:1
        packetId = buffer.readByte();
        catalog = buffer.readLenencStringBytes();
        schema = buffer.readLenencStringBytes();
        table = buffer.readLenencStringBytes();
        orgTable = buffer.readLenencStringBytes();
        name = buffer.readLenencStringBytes();
        System.out.println(new String(name));
        orgName = buffer.readLenencStringBytes();
        nextLength = buffer.readByte();
        charsetSet = (int)buffer.readFixInt(2);
        columnLength = buffer.readFixInt(4);
        type = (byte)(buffer.readByte() & 0xff);
        flags = (int)buffer.readFixInt(2);
        decimals = buffer.readByte();
        filler = (int)buffer.readFixInt(2);
//        if (!buffer.readFinished()) {
//            defaultValues = buffer.readLenencStringBytes();
//        }
    }

    @Override
    public void write(ProxyBuffer buffer) {
        // write packet length
        buffer.writeFixInt(3, packetLength);
        // write packet number
        buffer.writeByte(packetId);
        // write payload
        buffer.writeLenencString(catalog);
        buffer.writeLenencString(schema);
        buffer.writeLenencString(table);
        buffer.writeLenencString(orgTable);
        buffer.writeLenencString(name);
        buffer.writeLenencString(orgName);
        buffer.writeByte(nextLength);
        buffer.writeFixInt(2, charsetSet);
        buffer.writeFixInt(4, columnLength);
        buffer.writeByte(type);
        buffer.writeFixInt(2, flags);
        buffer.writeByte(decimals);
        buffer.writeFixInt(2, filler);
//        if (defaultValues != null) {
//            buffer.writeLenencString(defaultValues);
//        }
    }

    @Override
    public int calcPacketSize() {

        int size = 0;
        size += (catalog == null ? 1 : BufferUtil.getLength(catalog));
        size += (schema == null ? 1 : BufferUtil.getLength(schema));
        size += (table == null ? 1 : BufferUtil.getLength(table));
        size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
        size += (name == null ? 1 : BufferUtil.getLength(name));
        size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
        size += 13;
//        if (defaultValues != null) {
//            size += BufferUtil.getLength(defaultValues);
//        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "Column Definition Packet";
    }
}
