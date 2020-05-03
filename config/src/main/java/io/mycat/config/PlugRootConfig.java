package io.mycat.config;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jamie12221
 * date 2019-05-20 12:12
 **/
@Data
public class PlugRootConfig {
    private LoadBalance loadBalance = new LoadBalance();
    private Sequence sequence = new Sequence();

    @Data
    public static class LoadBalance {
        String defaultLoadBalance = "balanceRandom";
        List<LoadBalanceConfig> loadBalances = new ArrayList<>();
    }


    @Data
    public static class LoadBalanceConfig {
        String name;
        String clazz;

        public LoadBalanceConfig() {
        }

        public LoadBalanceConfig(String name, String clazz) {
            this.name = name;
            this.clazz = clazz;
        }
    }

    @Data
    public static class Sequence {
        List<SequenceConfig> sequences = new ArrayList<>();
    }

    @Data
    public static class SequenceConfig {
        String name;
        String clazz;
        String args;
    }
}
