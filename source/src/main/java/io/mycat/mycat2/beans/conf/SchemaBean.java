package io.mycat.mycat2.beans.conf;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc:
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class SchemaBean {
    public enum SchemaTypeEnum {
        // 所有表在一个MySQL Server上（但不分片），
        DB_IN_ONE_SERVER,
        // 所有表在不同的MySQL Server上（但不分片），
        DB_IN_MULTI_SERVER,
        // 只使用基于SQL注解的路由模式（高性能但手工指定）
        ANNOTATION_ROUTE;
        // 使用SQL解析的方式去判断路由
//        SQL_PARSE_ROUTE;
    }

    public String name;
    public SchemaTypeEnum schemaType;
    private String defaultDataNode;
    private List<TableDefBean> tables = new ArrayList<TableDefBean>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SchemaTypeEnum getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(SchemaTypeEnum schemaType) {
        this.schemaType = schemaType;
    }

    public String getDefaultDataNode() {
        return defaultDataNode;
    }

    public void setDefaultDataNode(String defaultDataNode) {
        this.defaultDataNode = defaultDataNode;
    }

    public List<TableDefBean> getTables() {
        return tables;
    }

    public void setTables(List<TableDefBean> tables) {
        this.tables = tables;
    }

    @Override
    public String toString() {
        return "SchemaBean{" + "name='" + name + '\'' + ", schemaType=" + schemaType + ", tables="
                + tables + '}';
    }
}
