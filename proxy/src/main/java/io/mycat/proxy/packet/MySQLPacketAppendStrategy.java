package io.mycat.proxy.packet;

import io.mycat.proxy.MycatExpection;

public interface MySQLPacketAppendStrategy {

    public void setCurrentPayloadMySQLPacket(MySQLPacket mySQLPacket);

    public  void appendFirstPacket(MySQLPacket current, MySQLPacket buffer, int startPos, int endPos, int remains) ;

    public default void appendFirstMultiPacket(MySQLPacket current, MySQLPacket buffer, int startPos, int endPos, int remains) {
        throw new MycatExpection("unsupport multi packet!");
    }

    public default void appendAfterMultiPacket(MySQLPacket current, MySQLPacket buffer, int startPos, int endPos, int remains) {
        throw new MycatExpection("unsupport multi packet!");
    }

    public default void appendEndMultiPacket(MySQLPacket current, MySQLPacket buffer, int startPos, int endPos, int remains) {
        throw new MycatExpection("unsupport multi packet!");
    }
}
