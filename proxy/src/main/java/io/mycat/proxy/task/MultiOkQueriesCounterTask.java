package io.mycat.proxy.task;

import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;

public class MultiOkQueriesCounterTask implements QueryResultSetTask {
    private int counter = 0;

    public MultiOkQueriesCounterTask(int counter) {
        this.counter = counter;
    }


    @Override
    public void onColumnDef(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onTextRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onBinaryRow(MySQLPacket mySQLPacket, int startPos, int endPos) {

    }

    @Override
    public void onFinished(boolean success, String errorMessage) {
        if (counter == 0) {
            AsynTaskCallBack<MySQLSession> callBack = getCurrentMySQLSession().getCallBackAndReset();
            callBack.finished(getCurrentMySQLSession(), this, true, null, errorMessage);
        } else {
            AsynTaskCallBack<MySQLSession> callBack = getCurrentMySQLSession().getCallBackAndReset();
            callBack.finished(getCurrentMySQLSession(), this, false, null,success? "couter fail":errorMessage);
        }
    }

    @Override
    public void onOk(MySQLPacket mySQLPacket, int startPos, int endPos) {
        counter--;
    }

    @Override
    public void onColumnCount(int columnCount) {

    }
}
