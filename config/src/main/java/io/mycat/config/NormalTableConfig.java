package io.mycat.config;

import lombok.*;

import javax.management.remote.rmi._RMIConnection_Stub;

@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode
public class NormalTableConfig {
    String createTableSQL;
    NormalBackEndTableInfoConfig dataNode;

    public static  NormalTableConfig create(String schemaName,
                                            String tableName,
                                            String createTableSQL,
                                            String targetName){
        NormalTableConfig normalTableConfig = new NormalTableConfig();
        NormalBackEndTableInfoConfig normalBackEndTableInfoConfig = new NormalBackEndTableInfoConfig();
        normalBackEndTableInfoConfig.setSchemaName(schemaName);
        normalBackEndTableInfoConfig.setTableName(tableName);
        normalBackEndTableInfoConfig.setTargetName(targetName);
        normalTableConfig.setDataNode(normalBackEndTableInfoConfig);
        normalTableConfig.setCreateTableSQL(createTableSQL);
        return normalTableConfig;
    }
}