package io.mycat;

public class StickyDataSourceNearnessImpl implements DataSourceNearness {
    final DataSourceNearness dataSourceNearness;
    long stickySessionTime;
    boolean lastMaster = false;
    long lastTime = 0;

    public StickyDataSourceNearnessImpl(DataSourceNearness dataSourceNearness, long stickySessionTime) {
        this.dataSourceNearness = dataSourceNearness;
        this.stickySessionTime = stickySessionTime;
    }

    @Override
    public String getDataSourceByTargetName(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        String dataSourceByTargetName;
        if (isOpenStickySessionTime()) {
            dataSourceByTargetName = stickSessionTimeMode(targetName, master, replicaBalanceType);
        } else {
            dataSourceByTargetName = stickSessionNearnessMode(targetName, master, replicaBalanceType);
        }
        lastMaster = master;
        return dataSourceByTargetName;
    }

    private String stickSessionNearnessMode(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        String dataSourceByTargetName;
        if (this.lastMaster) {
            dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, true, replicaBalanceType);
        } else {
            dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, master, replicaBalanceType);
        }
        return dataSourceByTargetName;
    }

    private String stickSessionTimeMode(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        String dataSourceByTargetName;
        if (master) {
            dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, true, replicaBalanceType);
            lastTime = System.currentTimeMillis();
        } else {
            if (lastMaster && isConsecutiveTime()) {
                dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, true, replicaBalanceType);
            } else {
                dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, master, replicaBalanceType);
            }
        }
        return dataSourceByTargetName;
    }

    private boolean isConsecutiveTime() {
        return (System.currentTimeMillis() - lastTime) < stickySessionTime;
    }

    private boolean isOpenStickySessionTime() {
        return stickySessionTime >= 0;
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
