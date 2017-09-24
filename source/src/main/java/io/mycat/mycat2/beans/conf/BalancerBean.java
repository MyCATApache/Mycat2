package io.mycat.mycat2.beans.conf;

/**
 * Desc: 负载均衡配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class BalancerBean {
    public enum BalancerStrategyEnum {
        RANDOM, ROUND_ROBIN, WEIGHT_RANDOM, WEIGHT_ROUND_ROBIN, RESPONSE_TIME, LEAST_CONNECTION, CAPACITY;
    }

    private boolean enable = false;
    private String ip = "0.0.0.0";
    private int port = 9066;
    private BalancerStrategyEnum strategy = BalancerStrategyEnum.RANDOM;

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

    public BalancerStrategyEnum getStrategy() {
        return strategy;
    }

    public void setStrategy(BalancerStrategyEnum strategy) {
        this.strategy = strategy;
    }

    @Override
    public String toString() {
        return "BalancerBean{" + "enable=" + enable + ", ip='" + ip + '\'' + ", port=" + port + ", strategy=" + strategy + '}';
    }
}
