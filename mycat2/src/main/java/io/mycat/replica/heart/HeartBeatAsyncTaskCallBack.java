package io.mycat.replica.heart;

import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.session.MySQLClientSession;

/**
 * @author : zhangwy
 * @version V1.0
 * @date Date : 2019年05月15日 21:30
 */
public abstract class HeartBeatAsyncTaskCallBack implements SessionCallBack<MySQLClientSession> {
    protected volatile boolean isQuit = false;
    protected final HeartbeatDetector heartbeatDetector ;
    public  HeartBeatAsyncTaskCallBack(HeartbeatDetector heartbeatDetector) {
        this.heartbeatDetector = heartbeatDetector;

    }

    public void setQuit(boolean quit) {
        isQuit = quit;
    }

  public void onStatus(int errorStatus) {
    DatasourceStatus datasourceStatus = new DatasourceStatus();
    heartbeatDetector.getHeartbeatManager()
        .setStatus(datasourceStatus, errorStatus);
  }
}
