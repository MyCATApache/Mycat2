package io.mycat.mycat2.beans;

/**
 * Desc: 对应mycat.yml文件中的balancer
 *
 * @date: 19/09/2017
 * @author: gaozhiwen
 */
public class BalancerBean {
    private boolean enable = false;
    /**
     * 负载均衡绑定的ip
     */
    private String ip = "0.0.0.0";
    /**
     * 负载均衡缓定的端口
     */
    private int port = 9066;
    /**
     * 负载均衡策略
     */
    private BalancerStrategy strategy;

    public enum BalancerStrategy {
        RANDOM, ROUND_ROBIN, WEIGHT_RANDOM, WEIGHT_ROUND_ROBIN, RESPONSE_TIME, LEAST_CONNECTION, CAPACITY;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public BalancerStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(BalancerStrategy strategy) {
        this.strategy = strategy;
    }
}
