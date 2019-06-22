package io.mycat.config.plug;

import io.mycat.config.ConfigurableRoot;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-20 12:12
 **/
public class PlugRootConfig implements ConfigurableRoot {

  String defaultLoadBalance;
  List<LoadBalanceConfig> loadBalances = new ArrayList<>();

  public List<LoadBalanceConfig> getLoadBalances() {
    return loadBalances;
  }

  public void setLoadBalances(List<LoadBalanceConfig> loadBalances) {
    this.loadBalances = loadBalances;
  }

  public String getDefaultLoadBalance() {
    return defaultLoadBalance;
  }

  public void setDefaultLoadBalance(String defaultLoadBalance) {
    this.defaultLoadBalance = defaultLoadBalance;
  }
}
