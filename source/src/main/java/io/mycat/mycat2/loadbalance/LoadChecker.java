package io.mycat.mycat2.loadbalance;

import io.mycat.proxy.man.AdminSession;

/**
 * 负载检查器接口,可实现不同的检查器，用于不同目的负载检查
 * <p>
 * Created by ynfeng on 2017/9/11.
 */
public interface LoadChecker {

    /**
     * 检查某个节点是否过载
     *
     * @param adminSession 节点的会话对象
     *
     * @return true已经过载, false未过载
     */
    boolean isOverLoad(AdminSession adminSession);

    /**
     * 查询某个节点的负载系数
     *
     * @param adminSession 节点的会话对象
     *
     * @return 负载系数，根据具体的场景定义数值含义
     */
    float getLoadFactor(AdminSession adminSession);
}
