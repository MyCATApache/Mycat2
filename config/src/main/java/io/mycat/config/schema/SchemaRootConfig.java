package io.mycat.config.schema;

import io.mycat.config.Configurable;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 对应schema.yml文件
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class SchemaRootConfig implements Configurable {
    private List<SchemaConfig> schemas;

    private List<DataNodeConfig> dataNodes = new ArrayList<DataNodeConfig>();

    public List<SchemaConfig> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SchemaConfig> schemas) {
        this.schemas = schemas;
    }

    public List<DataNodeConfig> getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(List<DataNodeConfig> dataNodes) {
        this.dataNodes = dataNodes;
    }
}
