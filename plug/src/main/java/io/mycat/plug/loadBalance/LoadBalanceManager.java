/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.plug.loadBalance;

import io.mycat.MycatException;
import io.mycat.config.PlugRootConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221 date 2019-05-20 12:21
 **/
public class LoadBalanceManager {

  private final Map<String, LoadBalanceStrategy> map = new HashMap<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadBalanceManager.class);
  private LoadBalanceStrategy defaultLoadBalanceStrategy = BalanceRandom.INSTANCE;

  public static LoadBalanceStrategy getLoadBalanceStrategy(String clazz)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    LoadBalanceStrategy o = null;
    Class<?> aClass = Class.forName(clazz);
    Method method = aClass.getMethod("values");
    Object[] invoke = (Object[]) method.invoke(null);
    o = (LoadBalanceStrategy) invoke[0];
    return o;
  }

  public void load(    PlugRootConfig.LoadBalance rootConfig) {
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(rootConfig);
    Objects
        .requireNonNull(rootConfig.getDefaultLoadBalance(), "defaultLoadBalance can not be empty");
    Objects.requireNonNull(rootConfig.getLoadBalances(), "loadBalances list is empty");
    ////////////////////////////////////check/////////////////////////////////////////////////

    for (PlugRootConfig.LoadBalanceConfig loadBalance : rootConfig.getLoadBalances()) {
      String clazz = loadBalance.getClazz();
      Objects.requireNonNull(clazz, "poolName can not be empty");
      try {
        LoadBalanceStrategy o = getLoadBalanceStrategy(clazz);
        map.put(loadBalance.getName(), o);
      } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
        LOGGER.error("can not load loadBalance:{}", clazz, e);
      }
    }
    defaultLoadBalanceStrategy = getLoadBalanceByBalanceName(rootConfig.getDefaultLoadBalance());
    if (defaultLoadBalanceStrategy == null) {
      throw new MycatException("can not load default loadBalance:{}",
          rootConfig.getDefaultLoadBalance());
    }
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    LoadBalanceStrategy strategy = map.get(name);
    if (strategy == null) {
      return defaultLoadBalanceStrategy;
    } else {
      return strategy;
    }
  }
}
