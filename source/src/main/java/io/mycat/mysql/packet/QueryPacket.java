package io.mycat.mysql.packet;

import io.mycat.proxy.ProxyBuffer;

/**
 * Created by ynfeng on 2017/8/13.
 */
public class QueryPacket extends MySQLPacket {
    public String sql;
    private byte pkgType = MySQLPacket.COM_QUERY;

    @Override
    public int calcPacketSize() {
        return sql.length() + 1;
    }

    @Override
    protected String getPacketInfo() {
        return "A COM_QUERY packet:" + sql;
    }

    @Override
    public void write(ProxyBuffer buffer) {
        buffer.writeFixInt(3, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(pkgType);
        buffer.writeFixString(sql);
    }
}
