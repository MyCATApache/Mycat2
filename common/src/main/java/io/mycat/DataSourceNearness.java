package io.mycat;

/**
 * @author Junwen Chen
 **/
public interface DataSourceNearness {
    public String getDataSourceByTargetName(String targetName, boolean master, ReplicaBalanceType replicaBalanceType);

    public void setLoadBalanceStrategy(String loadBalanceStrategy);

    public void clear();
}