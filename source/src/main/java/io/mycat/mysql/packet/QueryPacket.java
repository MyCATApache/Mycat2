package io.mycat.mysql.packet;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.proxy.ProxyBuffer;

/**
 * Created by ynfeng on 2017/8/13.
 */
public class QueryPacket extends MySQLPacket {
    public String sql;
    private byte pkgType = MySQLCommand.COM_QUERY;

    public QueryPacket(String sql, byte pkgType) {
        this.sql = sql;
        this.pkgType = pkgType;
    }

    public QueryPacket() {
    }

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
