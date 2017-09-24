package io.mycat.mycat2.beans.conf;

import io.mycat.proxy.Configurable;

/**
 * Desc: 对应mycat.yml文件
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class HeartbeatConfig implements Configurable {
    private HeartbeatBean heartbeat;

    public HeartbeatBean getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(HeartbeatBean heartbeat) {
        this.heartbeat = heartbeat;
    }
}
