package io.mycat.mysql.packet;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.BufferUtil;

/**
 * <pre>
 * @see https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
 * </pre>
 *
 * @author linxiaofang
 * @date 2018/11/12
 */
public class ColumnDefPacket extends MySQLPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();

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
    public byte[] defaultValues;
    public boolean comFieldList;

    public ColumnDefPacket(boolean comFieldList) {
       this.comFieldList = comFieldList;
    }
    public ColumnDefPacket() {
        this.comFieldList = false;
    }

    @Override
    public void writePayload(ProxyBuffer buffer) {
        buffer.writeLenencBytesWithNullable(catalog);
        buffer.writeLenencBytesWithNullable(schema);
        buffer.writeLenencBytesWithNullable(table);
        buffer.writeLenencBytesWithNullable(orgTable);
        buffer.writeLenencBytesWithNullable(name);
        buffer.writeLenencBytesWithNullable(orgName);
        buffer.writeByte(nextLength);
        buffer.writeFixInt(2, charsetSet);
        buffer.writeFixInt(4, columnLength);
        buffer.writeByte(type);
        buffer.writeFixInt(2, flags);
        buffer.writeByte(decimals);
        buffer.writeByte((byte) 0x00);//filler
        buffer.writeByte((byte) 0x00);//filler
        if (defaultValues != null) {
            buffer.writeLenencString(defaultValues);
        }
    }

    public void readPayload(ProxyBuffer buffer) {
        catalog = buffer.readLenencStringBytes();
        schema = buffer.readLenencStringBytes();
        table = buffer.readLenencStringBytes();
        orgTable = buffer.readLenencStringBytes();
        name = buffer.readLenencStringBytes();
        orgName = buffer.readLenencStringBytes();
        nextLength = buffer.readByte();
        charsetSet = (int) buffer.readFixInt(2);
        columnLength = buffer.readFixInt(4);
        type = (byte) (buffer.readByte() & 0xff);
        flags = (int) buffer.readFixInt(2);
        decimals = buffer.readByte();
        if (buffer.readByte() != 0x00 && buffer.readByte() != 0x00) {//filler
            throw new IllegalArgumentException("ColumnDefPacket read fail!!!:" + this);
        }
        if (comFieldList) {
            defaultValues = buffer.readLenencStringBytes();
        }
    }

    @Override
    public int calcPayloadSize() {
        int size = 0;
        size += (catalog == null ? 1 : BufferUtil.getLength(catalog));
        size += (schema == null ? 1 : BufferUtil.getLength(schema));
        size += (table == null ? 1 : BufferUtil.getLength(table));
        size += (orgTable == null ? 1 : BufferUtil.getLength(orgTable));
        size += (name == null ? 1 : BufferUtil.getLength(name));
        size += (orgName == null ? 1 : BufferUtil.getLength(orgName));
        size += 13;
        if (defaultValues != null) {
            size += BufferUtil.getLength(defaultValues);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "Column Definition Packet";
    }


}
