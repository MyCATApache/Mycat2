package io.mycat.replica.heart;

import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.session.MySQLClientSession;

/**
 * @author : zhangwy
 * @version V1.0
 * @date Date : 2019年05月15日 21:30
 */
public abstract class HeartBeatAsyncTaskCallBack  implements AsyncTaskCallBack<MySQLClientSession>{
    protected volatile boolean isQuit = false;
    protected HeartbeatDetector heartbeatDetector = null;
    public  HeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
        this.heartbeatDetector = heartbeatDetector;

    }

    public void setQuit(boolean quit) {
        isQuit = quit;
    }
}
