package io.mycat.mycat2.beans.conf;

import java.util.ArrayList;
import java.util.List;

import io.mycat.proxy.Configurable;

/**
 * Desc: 对应schema.yml文件
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class SchemaConfig implements Configurable {
    private List<SchemaBean> schemas;

    private List<DNBean> dataNodes = new ArrayList<DNBean>();

    public List<SchemaBean> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SchemaBean> schemas) {
        this.schemas = schemas;
    }

    public List<DNBean> getDataNodes() {
        return dataNodes;
    }

    public void setDataNodes(List<DNBean> dataNodes) {
        this.dataNodes = dataNodes;
    }
}
