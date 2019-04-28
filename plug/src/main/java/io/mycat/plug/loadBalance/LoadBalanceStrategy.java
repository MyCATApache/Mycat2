package io.mycat.plug.loadBalance;

import java.util.List;

@FunctionalInterface
public interface LoadBalanceStrategy {
    <INFO,ENTITY> ENTITY select(INFO replica,int writeIndex,List<ENTITY> datasources);
}
