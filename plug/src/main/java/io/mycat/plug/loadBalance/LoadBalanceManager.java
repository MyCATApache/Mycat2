/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.plug.loadBalance;

import io.mycat.config.plug.LoadBalanceConfig;
import io.mycat.config.plug.PlugRootConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-20 12:21
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
