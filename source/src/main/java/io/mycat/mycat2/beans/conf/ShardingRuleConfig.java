package io.mycat.mycat2.beans.conf;

import java.util.List;
import java.util.Map;

/**
 * Desc: 对应sharding-rule.yml文件
 *
 * @date: 23/09/2017
 * @author: gaozhiwen
 */
public class ShardingRuleConfig {
    private List<ShardingRuleBean> shardingRules;

    public List<ShardingRuleBean> getShardingRules() {
        return shardingRules;
    }

    public void setShardingRules(List<ShardingRuleBean> shardingRules) {
        this.shardingRules = shardingRules;
    }

    public static class ShardingRuleBean {
        private String name;
        private String algorithm;
        private Map<String, String> params;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public Map<String, String> getParams() {
            return params;
        }

        public void setParams(Map<String, String> params) {
            this.params = params;
        }
    }
}
