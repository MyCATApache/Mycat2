package io.mycat.plug.loadBalance;


import java.util.List;

/**
 * 最小连接数优先
 */
public enum BalanceLeastActive implements LoadBalanceStrategy{
  INSTANCE {
    @Override
    public  LoadBalanceDataSource select(LoadBalanceInfo info, List<LoadBalanceDataSource> entityList) {
      if(null == entityList && entityList.size() == 0) {
        return null;
      }
      int len = entityList.size();
      int leastIndex = 0 ;
      int leastActive = entityList.get(0).getSessionCounter();
      for (int i = 0; i < len; i++) {
        if(leastActive > entityList.get(i).getSessionCounter()) {
           leastActive = entityList.get(i).getSessionCounter();
           leastIndex = i;
        }
      }
      return entityList.get(leastIndex);
    }
  };
}

