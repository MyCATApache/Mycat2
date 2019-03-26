package io.mycat.mysql.packet;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.proxy.ProxyBuffer;

/**
 * Created by cjw on 2017/8/13.
 */
public class ComQueryPacket extends MySQLPacket {
    public String sql;
    @Override
    public int calcPayloadSize() {
        return sql.length() + 1;
    }

    @Override
    protected String getPacketInfo() {
        return "A COM_QUERY packet:" + new String(sql);
    }

    @Override
    public void writePayload(ProxyBuffer buffer) {
        buffer.writeByte(MySQLCommand.COM_QUERY);
        buffer.writeFixString(sql);
    }

    @Override
    public void readPayload(ProxyBuffer buffer) {
        buffer.readByte();
        sql = buffer.readEOFString();
    }
}
