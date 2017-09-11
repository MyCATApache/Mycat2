package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 轮询策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class RoundRobinStrategy implements LoadBalanceStrategy{
    @Override
    public AdminSession get(Object attachement) {
        return null;
    }
}
