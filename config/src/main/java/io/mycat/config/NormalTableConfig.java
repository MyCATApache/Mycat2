package io.mycat.config;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class NormalTableConfig {
    String createTableSQL;
    NormalBackEndTableInfoConfig dataNode;
}