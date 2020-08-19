package io.mycat.config;

import lombok.*;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class NormalTableConfig {
    String createTableSQL;
    BackEndTableInfoConfig dataNode;

    @AllArgsConstructor
    @Data
    @Builder
    @EqualsAndHashCode
    public static class BackEndTableInfoConfig {
        private String targetName;
        private String schemaName;
        private String tableName;

        public BackEndTableInfoConfig() {

        }
    }
}