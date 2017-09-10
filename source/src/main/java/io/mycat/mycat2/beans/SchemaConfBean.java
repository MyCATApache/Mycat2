package io.mycat.mycat2.beans;

import java.util.List;

/**
 * Desc: 用于加载schema.yml的类
 *
 * @date: 10/09/2017
 * @author: gaozhiwen
 */
public class SchemaConfBean {
    private List<SchemaBean> schemas;

    public List<SchemaBean> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SchemaBean> schemas) {
        this.schemas = schemas;
    }
}
