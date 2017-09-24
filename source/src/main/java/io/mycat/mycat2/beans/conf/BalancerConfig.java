package io.mycat.mycat2.beans.conf;

import io.mycat.proxy.Configurable;

/**
 * Desc: 对应mycat.yml文件，用于负载均衡，只有在集群模式启动下才生效
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class BalancerConfig implements Configurable {
    private BalancerBean balancer;

    public BalancerBean getBalancer() {
        return balancer;
    }

    public void setBalancer(BalancerBean balancer) {
        this.balancer = balancer;
    }
}
