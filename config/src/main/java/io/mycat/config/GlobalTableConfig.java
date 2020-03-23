package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class GlobalTableConfig {
    String createTableSQL;
    String balance;
    List<ShardingQueryRootConfig.BackEndTableInfoConfig> dataNodes;
    List<ShardingQueryRootConfig.BackEndTableInfoConfig> readOnlyDataNodes;
}