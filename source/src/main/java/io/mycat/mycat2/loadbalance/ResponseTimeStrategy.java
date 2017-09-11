package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 基于平均响应时间的均衡策略
 *
 * Created by ynfeng on 2017/9/12.
 */
public class ResponseTimeStrategy implements LoadBalanceStrategy {
    @Override
    public AdminSession get(Object attachement) {
        return null;
    }
}
