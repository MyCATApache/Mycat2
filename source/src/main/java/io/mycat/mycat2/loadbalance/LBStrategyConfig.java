package io.mycat.mycat2.loadbalance;

import io.mycat.mycat2.beans.conf.BalancerBean.BalancerStrategyEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ynfeng on 2017/9/25.
 */
public class LBStrategyConfig {
    private final static Map<BalancerStrategyEnum, LoadBalanceStrategy> strategyMap = new HashMap<>();

    static {
        strategyMap.put(BalancerStrategyEnum.RANDOM, new RandomStrategy());
        strategyMap.put(BalancerStrategyEnum.CAPACITY, new CapacityStrategy());
        strategyMap.put(BalancerStrategyEnum.LEAST_CONNECTION, new LeastConnectionStrategy());
        strategyMap.put(BalancerStrategyEnum.RESPONSE_TIME, new ResponseTimeStrategy());
        strategyMap.put(BalancerStrategyEnum.ROUND_ROBIN, new RoundRobinStrategy());
        strategyMap.put(BalancerStrategyEnum.WEIGHT_RANDOM, new WeightedRandomStrategy());
        strategyMap.put(BalancerStrategyEnum.WEIGHT_ROUND_ROBIN, new WeightedRoundRobinStrategy());
    }

    public static LoadBalanceStrategy getStrategy(BalancerStrategyEnum balancerStrategyEnum) {
        LoadBalanceStrategy loadBalanceStrategy = strategyMap.get(balancerStrategyEnum);
        if (loadBalanceStrategy == null) {
            throw new IllegalArgumentException("不支持的LB策略" + balancerStrategyEnum);
        }
        return loadBalanceStrategy;
    }
}
