package io.mycat;

public class StickyDataSourceNearnessImpl implements DataSourceNearness {
    final DataSourceNearness dataSourceNearness;
    boolean lastMaster = false;


    public StickyDataSourceNearnessImpl(DataSourceNearness dataSourceNearness) {
        this.dataSourceNearness = dataSourceNearness;
    }

    @Override
    public String getDataSourceByTargetName(String targetName, boolean master, ReplicaBalanceType replicaBalanceType) {
        String dataSourceByTargetName;
        if (lastMaster){
            dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, true, replicaBalanceType);
        }else {
            dataSourceByTargetName = this.dataSourceNearness.getDataSourceByTargetName(targetName, master, replicaBalanceType);
        }
        lastMaster = master;
        return dataSourceByTargetName;
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
