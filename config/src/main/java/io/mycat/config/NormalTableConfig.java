package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class NormalTableConfig {
    String createTableSQL;
    NormalBackEndTableInfoConfig locality = new NormalBackEndTableInfoConfig();

    public static NormalTableConfig create(String schemaName,
                                           String tableName,
                                           String createTableSQL,
                                           String targetName) {
        NormalTableConfig normalTableConfig = new NormalTableConfig();
        NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig();
        normalBackEndTableInfoConfig.setSchemaName(schemaName);
        normalBackEndTableInfoConfig.setTableName(tableName);
        normalBackEndTableInfoConfig.setTargetName(targetName);
        normalTableConfig.setLocality(normalBackEndTableInfoConfig);
        normalTableConfig.setCreateTableSQL(createTableSQL);
        return normalTableConfig;
    }
}