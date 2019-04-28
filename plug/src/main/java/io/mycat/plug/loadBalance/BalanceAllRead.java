package io.mycat.plug.loadBalance;


import java.util.List;

public enum  BalanceAllRead implements LoadBalanceStrategy{
    INSTANCE;
    @Override
    public <GLOBAL_INFO, ENTITY> ENTITY select(GLOBAL_INFO replica, int writeIndex, List<ENTITY> datasources) {
        int size = datasources.size();
        for (int i = 0; i < size; i++) {
            if (writeIndex == i) {
                continue;
            }
            return datasources.get(i);
        }
        return null;
    }
}
