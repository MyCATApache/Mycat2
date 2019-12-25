package io.mycat.plug;

import io.mycat.MycatConfig;
import io.mycat.config.PlugRootConfig;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;

import java.util.Objects;

public enum PlugRuntime {
  INSTCANE;
  volatile LoadBalanceManager manager;
  volatile MycatConfig mycatConfig;

  PlugRuntime() {

  }

  public void load(MycatConfig mycatConfig) {
    if (this.mycatConfig  == null||this.mycatConfig != mycatConfig) {
      PlugRootConfig plugRootConfig = mycatConfig.getPlug();
      Objects.requireNonNull(plugRootConfig, "plug config can not found");
      LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
      loadBalanceManager.load(plugRootConfig);
      this.manager = loadBalanceManager;
    }
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return manager.getLoadBalanceByBalanceName(name);
  }
}