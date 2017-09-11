package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 均衡策略接口
 *
 * Created by ynfeng on 2017/9/12.
 */
public interface LoadBalanceStrategy {

    /**
     * 按照策略获取一个节点
     * @param attachement 附加参数
     * @return AdminSession对象
     */
    AdminSession get(Object attachement);
}
