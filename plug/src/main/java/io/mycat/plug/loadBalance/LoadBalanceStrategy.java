package io.mycat.plug.loadBalance;

import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-13 14:45 负载均衡算法接口
 **/
@FunctionalInterface
public interface LoadBalanceStrategy {

  /**
   * @param info 全局信息
   * @param entityList 可选列表
   */
  LoadBalanceDataSource select(LoadBalanceInfo info, List<LoadBalanceDataSource> entityList);
}
