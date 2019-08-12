package io.mycat.plug;

import io.mycat.ConfigRuntime;
import io.mycat.config.ConfigFile;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.Objects;

public enum PlugRuntime {
  INSTCANE;
  volatile LoadBalanceManager manager;

  PlugRuntime() {

  }

  public void load() {
    PlugRootConfig plugRootConfig = ConfigRuntime.INSTCANE.getConfig(ConfigFile.PLUG);
    Objects.requireNonNull(plugRootConfig, "plug config can not found");
    LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
    loadBalanceManager.load(plugRootConfig);
    this.manager = loadBalanceManager;
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return manager.getLoadBalanceByBalanceName(name);
  }
}