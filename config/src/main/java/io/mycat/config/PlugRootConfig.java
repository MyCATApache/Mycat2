package io.mycat.config;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jamie12221
 * date 2019-05-20 12:12
 **/
@Data
@EqualsAndHashCode
public class PlugRootConfig {
    private LoadBalance loadBalance = new LoadBalance();
    private Sequence sequence = new Sequence();
    private Hint hint = new Hint();
    private MycatCommand command = new MycatCommand();
    private List<String> extra = new ArrayList<>();

    @Data
    @EqualsAndHashCode
    public static class LoadBalance {
        String defaultLoadBalance = "balanceRandom";
        List<LoadBalanceConfig> loadBalances = new ArrayList<>();
    }


    @Data
    @EqualsAndHashCode
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
    @EqualsAndHashCode
    public static class Sequence {
        List<SequenceConfig> sequences = new ArrayList<>();
    }

    @Data
    @EqualsAndHashCode
    public static class SequenceConfig {
        String name;
        String clazz;
        String args;

        public SequenceConfig(String name, String clazz, String args) {
            this.name = name;
            this.clazz = clazz;
            this.args = args;
        }

        public SequenceConfig() {
        }
    }

    @Data
    @EqualsAndHashCode
    public static class Hint {
        List<HintConfig> hints = new ArrayList<>();
    }

    @Data
    @EqualsAndHashCode
    public static class HintConfig {
        String name;
        String clazz;
        String args;

        public HintConfig() {
        }

        public HintConfig(String name, String clazz, String args) {
            this.name = name;
            this.clazz = clazz;
            this.args = args;
        }
    }

    @Data
    @EqualsAndHashCode
    public static class MycatCommand {
        List<MycatCommandConfig> commands = new ArrayList<>();
    }

    @Data
    @Builder
    @EqualsAndHashCode
    public static class MycatCommandConfig {
        String name;
        String clazz;

        public MycatCommandConfig(String name, String clazz) {
            this.name = name;
            this.clazz = clazz;
        }

        public MycatCommandConfig() {
        }
    }
}
