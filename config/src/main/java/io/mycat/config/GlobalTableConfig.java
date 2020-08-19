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
    List<BackEndTableInfoConfig> dataNodes;

    @AllArgsConstructor
    @Data
    @Builder
    @EqualsAndHashCode
    public static class BackEndTableInfoConfig {
        private String targetName;

        public BackEndTableInfoConfig() {
        }
    }
}