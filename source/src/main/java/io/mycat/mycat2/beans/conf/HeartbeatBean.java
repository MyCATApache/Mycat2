package io.mycat.mycat2.beans.conf;

/**
 * Desc: 心跳配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class HeartbeatBean {
    private int timerExecutor = 2;
    /**
     * 默认复制组心跳周期
     */
    private long replicaHeartbeatPeriod = 10 * 1000L;
    /**
     * 默认复制组 空闲检查周期
     */
    private long replicaIdleCheckPeriod = 5 * 60 * 1000L;
    /**
     * 默认空闲超时时间
     */
    private long idleTimeout = 30 * 60 * 1000L;
    private long processorCheckPeriod = 1 * 1000L;;
    private long minSwitchtimeInterval = 30 * 60 * 1000L;;

    public int getTimerExecutor() {
        return timerExecutor;
    }

    public void setTimerExecutor(int timerExecutor) {
        this.timerExecutor = timerExecutor;
    }

    public long getReplicaHeartbeatPeriod() {
        return replicaHeartbeatPeriod;
    }

    public void setReplicaHeartbeatPeriod(long replicaHeartbeatPeriod) {
        this.replicaHeartbeatPeriod = replicaHeartbeatPeriod;
    }

    public long getReplicaIdleCheckPeriod() {
        return replicaIdleCheckPeriod;
    }

    public void setReplicaIdleCheckPeriod(long replicaIdleCheckPeriod) {
        this.replicaIdleCheckPeriod = replicaIdleCheckPeriod;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public long getProcessorCheckPeriod() {
        return processorCheckPeriod;
    }

    public void setProcessorCheckPeriod(long processorCheckPeriod) {
        this.processorCheckPeriod = processorCheckPeriod;
    }

    public long getMinSwitchtimeInterval() {
        return minSwitchtimeInterval;
    }

    public void setMinSwitchtimeInterval(long minSwitchtimeInterval) {
        this.minSwitchtimeInterval = minSwitchtimeInterval;
    }

    @Override
    public String toString() {
        return "HeartbeatBean{" + "timerExecutor=" + timerExecutor + ", replicaHeartbeatPeriod=" + replicaHeartbeatPeriod
                + ", replicaIdleCheckPeriod=" + replicaIdleCheckPeriod + ", idleTimeout=" + idleTimeout + ", processorCheckPeriod="
                + processorCheckPeriod + ", minSwitchtimeInterval=" + minSwitchtimeInterval + '}';
    }
}
