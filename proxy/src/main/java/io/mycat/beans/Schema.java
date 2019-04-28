package io.mycat.beans;

import io.mycat.config.MycatConfig;
import io.mycat.config.schema.SchemaConfig;
import io.mycat.proxy.MycatRuntime;

public class Schema {
    final SchemaConfig schemaConfig;
    final DataNode defaultDataNode;

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public Schema(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        MycatConfig mycatConfig = MycatRuntime.INSTANCE.getMycatConfig();
        defaultDataNode = mycatConfig.getDataNodeMap().get(schemaConfig.getDefaultDataNode());
    }

    public DataNode getDefaultDataNode() {
        return defaultDataNode;
    }

    public String getSchemaName() {
        return schemaConfig.getName();
    }
}
