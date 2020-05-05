package io.mycat;
/**
 * @author Junwen Chen
 **/
public interface DataSourceNearness {

    public String getDataSourceByTargetName(String targetName);

    public void setLoadBalanceStrategy(String loadBalanceStrategy);

    public void setUpdate(boolean update);

    public void clear();
}