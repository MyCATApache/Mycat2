package io.mycat.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jamie12221
 * date 2019-05-20 12:12
 **/
@Data
public class PlugRootConfig {
    private LoadBalance loadBalance = new LoadBalance();

    @Data
    public static class LoadBalance {
        String defaultLoadBalance;
        List<LoadBalanceConfig> loadBalances = new ArrayList<>();
    }

    @Data
    public static class LoadBalanceConfig {
        String name;
        String clazz;
    }
}
