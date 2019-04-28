package io.mycat.proxy.task.prepareStatement;

import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.ResultSetTask;

import java.io.IOException;

public class CloseTask implements ResultSetTask {
    public void request(MySQLSession mysql, long statementId, AsynTaskCallBack<MySQLSession> callBack) {
        request(mysql, 0x19,statementId, callBack);
    }

    @Override
    public void onWriteFinished(MySQLSession mysql) throws IOException {
        clearAndFinished(true,null);
    }
}
