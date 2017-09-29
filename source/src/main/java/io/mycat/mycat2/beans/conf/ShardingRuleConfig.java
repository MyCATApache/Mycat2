package io.mycat.mycat2.beans.conf;

import java.util.List;

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
}
