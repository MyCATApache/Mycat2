package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class NormalTableConfig {
    String createTableSQL;
    ShardingQueryRootConfig.BackEndTableInfoConfig dataNode;
}