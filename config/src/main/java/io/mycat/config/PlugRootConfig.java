package io.mycat.config;

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

    private Sequence sequence = new Sequence();


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

}
