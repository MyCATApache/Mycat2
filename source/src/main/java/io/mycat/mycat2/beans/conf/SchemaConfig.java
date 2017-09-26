package io.mycat.mycat2.beans.conf;

import io.mycat.proxy.Configurable;

import java.util.List;

/**
 * Desc: 对应schema.yml文件
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class SchemaConfig implements Configurable {
    private List<SchemaBean> schemas;

    public List<SchemaBean> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SchemaBean> schemas) {
        this.schemas = schemas;
    }
}
