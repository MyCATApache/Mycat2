package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class ShardingTableConfig {
    List<ShardingQueryRootConfig.BackEndTableInfoConfig> dataNodes;
    SharingFuntionRootConfig.ShardingFuntion function;
    String createTableSQL;

    public ShardingTableConfig() {
    }
}