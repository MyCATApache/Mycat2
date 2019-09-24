package io.mycat.config.pattern;

import io.mycat.config.ConfigurableRoot;

import java.util.List;

public class PatternRootConfig extends ConfigurableRoot {
    private String defaultSchema;
    private List<TextItemConfig> sql;
    private List<SchemaConfig> schemas;

    public static class TextItemConfig {
        String sql;
        String code;

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }
    }

    public static class SchemaConfig {
        String schema;
        String defaultCode;
        List<TextItemConfig> sql;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
    }

    public List<TextItemConfig> getSql() {
        return sql;
    }

    public void setSql(List<TextItemConfig> sql) {
        this.sql = sql;
    }

    public List getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SchemaConfig> schemas) {
        this.schemas = schemas;
    }
}