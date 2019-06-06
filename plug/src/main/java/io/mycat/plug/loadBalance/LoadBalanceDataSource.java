package io.mycat.plug.loadBalance;

/**
 * @author : zhangwy
 * @version V1.0
 * date Date : 2019年06月01日 14:02
 */
public interface LoadBalanceDataSource {

    String getName();


    boolean isMaster();

    boolean isSlave();

    int getSessionCounter();

    int getWeight();
}
