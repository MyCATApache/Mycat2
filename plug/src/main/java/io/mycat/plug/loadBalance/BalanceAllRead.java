package io.mycat.plug.loadBalance;


import java.util.List;

/**
 *
 */
public enum  BalanceAllRead implements LoadBalanceStrategy{
  INSTANCE;

  @Override
  public <GLOBAL_INFO, ENTITY> ENTITY select(GLOBAL_INFO info, int excludeIndex,
      List<ENTITY> entityList) {
    int size = entityList.size();
    for (int i = 0; i < size; i++) {
      if (excludeIndex == i) {
        continue;
      }
      return entityList.get(i);
    }
    return null;
  }
}
