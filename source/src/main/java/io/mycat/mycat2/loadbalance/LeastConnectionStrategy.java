package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 最少连接的均衡策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class LeastConnectionStrategy implements LoadBalanceStrategy{
    @Override
    public AdminSession get(Object attachement) {
        return null;
    }
}
