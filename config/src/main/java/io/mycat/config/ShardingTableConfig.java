package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Data
@Builder
public class ShardingTableConfig {
    List<ShardingQueryRootConfig.BackEndTableInfoConfig> dataNodes;
    List<ShardingQueryRootConfig.Column> columns = new ArrayList<>();
    String createTableSQL;

    public ShardingTableConfig() {
    }
}