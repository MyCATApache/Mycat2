package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 基于服务器能力的（如cpu,内存等使用率）均衡策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class CapacityStrategy implements LoadBalanceStrategy{
    @Override
    public AdminSession get(Object attachement) {
        return null;
    }
}
