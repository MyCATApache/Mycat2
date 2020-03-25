package io.mycat.plug;

import io.mycat.MycatConfig;
import io.mycat.config.PlugRootConfig;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.plug.sequence.SequenceSnowflakeGenerator;
import lombok.SneakyThrows;

import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.function.Supplier;

public enum PlugRuntime {
  INSTCANE;
  volatile LoadBalanceManager manager;
  volatile MycatConfig mycatConfig;

  PlugRuntime() {

  }

  @SneakyThrows
  public void load(MycatConfig mycatConfig) {
    if (this.mycatConfig  == null||this.mycatConfig != mycatConfig) {
      PlugRootConfig plugRootConfig = mycatConfig.getPlug();
      Objects.requireNonNull(plugRootConfig, "plug config can not found");
      LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
      loadBalanceManager.load(mycatConfig.getPlug().getLoadBalance());
      this.manager = loadBalanceManager;


      SequenceGenerator.INSTANCE.register("snowflake",new SequenceSnowflakeGenerator("workerId:1"));
      SequenceGenerator.INSTANCE.register("GLOBAL",new SequenceSnowflakeGenerator("workerId:1"));
      for (PlugRootConfig.SequenceConfig sequence : plugRootConfig.getSequence().getSequences()) {
        String name = sequence.getName();
        String clazz = sequence.getClazz();
        String args = sequence.getArgs();
        Constructor<?> declaredConstructor = Class.forName(clazz).getDeclaredConstructor(String.class);
        Object o = declaredConstructor.newInstance(args);
        SequenceGenerator.INSTANCE.register(name,(Supplier) o);
      }
    }
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return manager.getLoadBalanceByBalanceName(name);
  }
}