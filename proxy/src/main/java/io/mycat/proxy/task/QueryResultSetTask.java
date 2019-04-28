package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;

public interface QueryResultSetTask extends ResultSetTask {

    default public void request(MySQLSession mysql, String sql, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, 3, sql,callBack);
    }

    @Override
    public abstract void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos);

    @Override
    public abstract void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos);

    @Override
    public abstract void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos);

}
