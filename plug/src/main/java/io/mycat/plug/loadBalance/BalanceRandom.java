package io.mycat.plug.loadBalance;


import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机算法
 */
public enum BalanceRandom implements LoadBalanceStrategy{
  INSTANCE {
    @Override
    public  LoadBalanceDataSource select(LoadBalanceInfo info, List<LoadBalanceDataSource> entityList) {
      if(null == entityList && entityList.size() == 0) {
          return null;
      }
      int size = entityList.size();
      int randomIndex = ThreadLocalRandom.current().nextInt(size);
      return entityList.get(randomIndex);
    }
  };
}
