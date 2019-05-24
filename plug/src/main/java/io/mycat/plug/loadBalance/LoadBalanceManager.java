package io.mycat.plug.loadBalance;

import io.mycat.config.plug.LoadBalanceConfig;
import io.mycat.config.plug.PlugRootConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-20 12:21
 **/
public class LoadBalanceManager {

  private final Map<String, LoadBalanceStrategy> map = new HashMap<>();

  public void load(PlugRootConfig rootConfig) {
    for (LoadBalanceConfig loadBalance : rootConfig.getLoadBalances()) {
      String clazz = loadBalance.getClazz();
      Class<?> aClass = null;
      try {
        aClass = Class.forName(clazz);
        Method method = aClass.getMethod("values");
        Object[] invoke = (Object[]) method.invoke(null);
        Object o = invoke[0];
        map.put(loadBalance.getName(), (LoadBalanceStrategy) o);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
      }
    }
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return map.get(name);
  }
}
