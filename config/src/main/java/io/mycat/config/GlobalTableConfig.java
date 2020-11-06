package io.mycat.config;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class GlobalTableConfig {
    String createTableSQL;
    String balance;
    List<GlobalBackEndTableInfoConfig> dataNodes;
}