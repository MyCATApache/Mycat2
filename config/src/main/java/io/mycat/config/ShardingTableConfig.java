package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@AllArgsConstructor
@Data
@Builder
@EqualsAndHashCode
public class ShardingTableConfig {
    List<ShardingBackEndTableInfoConfig> dataNodes;
    ShardingFuntion function;
    String createTableSQL;

    public ShardingTableConfig() {
    }
}