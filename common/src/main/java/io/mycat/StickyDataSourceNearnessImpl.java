package io.mycat;

public class StickyDataSourceNearnessImpl implements DataSourceNearness {
    final DataSourceNearness dataSourceNearness;
    long stickySessionTime;
    boolean lastMaster = false;
    long lastTime = System.currentTimeMillis();

    public StickyDataSourceNearnessImpl(DataSourceNearness dataSourceNearness, long stickySessionTime) {
        this.dataSourceNearness = dataSourceNearness;
        this.stickySessionTime = stickySessionTime;
    }

    @Override
    public String getDataSourceByTargetName(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        String dataSourceByTargetName;
        if (master) {
            dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, true, replicaBalanceType);
            lastTime = System.currentTimeMillis();
        } else {
            if (lastMaster && isConsecutiveTime()) {
                dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, true, replicaBalanceType);
            } else {
                dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, false, replicaBalanceType);
            }
        }
        lastMaster = master;
        return dataSourceByTargetName;
    }

    private boolean isConsecutiveTime() {
        return (System.currentTimeMillis()- lastTime) < stickySessionTime;
    }

    @Override
    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.dataSourceNearness.setLoadBalanceStrategy(loadBalanceStrategy);
    }

    @Override
    public void clear() {
        this.dataSourceNearness.clear();
    }
}
