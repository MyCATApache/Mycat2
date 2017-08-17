package io.mycat.mysql.packet;

import io.mycat.proxy.ProxyBuffer;

/**
 * Created by zhangwy on 2017/8/14.
 */
public class INITDBPacket extends MySQLPacket {
    public String sql;
    private byte pkgType = MySQLPacket.COM_INIT_DB;

    @Override
    public int calcPacketSize() {
        return sql.length() + 1;
    }

    @Override
    protected String getPacketInfo() {
        return "A COM_INIT_DB packet:" + sql;
    }

    @Override
    public void write(ProxyBuffer buffer) {
        buffer.writeFixInt(3, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(pkgType);
        buffer.writeFixString(sql);
    }
}
