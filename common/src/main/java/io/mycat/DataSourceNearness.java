package io.mycat;

public interface DataSourceNearness {

    public String getDataSourceByTargetName(String targetName);

    public void setLoadBalanceStrategy(String loadBalanceStrategy);

    public void setUpdate(boolean update);

    public void clear();
}