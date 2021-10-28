package io.mycat;

import io.mycat.config.LogicSchemaConfig;
import io.mycat.prototypeserver.mysql.PrototypeService;

import java.util.Map;

public class MysqlMetadataManager extends MetadataManager {
    public MysqlMetadataManager(Map<String,LogicSchemaConfig> schemaConfigs, PrototypeService prototypeService) {
        super((schemaConfigs),prototypeService);
    }
}
