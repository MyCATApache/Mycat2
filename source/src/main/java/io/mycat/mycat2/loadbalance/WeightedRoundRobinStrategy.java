package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 带权重的轮询
 *
 * Created by ynfeng on 2017/9/12.
 */
public class WeightedRoundRobinStrategy implements LoadBalanceStrategy {
    @Override
    public AdminSession get(Object attachement) {
        return null;
    }
}
