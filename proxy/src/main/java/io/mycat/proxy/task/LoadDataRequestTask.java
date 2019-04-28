package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;

public class LoadDataRequestTask implements ResultSetTask {
    String fileName;

    @Override
    public void onFinished(boolean success, String errorMessage) {
        MySQLSession currentMySQLSession = getCurrentMySQLSession();
        AsynTaskCallBack<MySQLSession> callBack = currentMySQLSession.getCallBackAndReset();
        callBack.finished(currentMySQLSession, this, success, fileName, errorMessage);
    }

    @Override
    public void onLoadDataRequest(MySQLPacket mySQLPacket, int startPos, int endPos) {
        fileName = mySQLPacket.getEOFString(startPos + 5);
        clearAndFinished(true,null);
    }
}
