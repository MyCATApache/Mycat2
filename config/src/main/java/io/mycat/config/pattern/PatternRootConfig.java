package io.mycat.config.pattern;

import io.mycat.config.ConfigurableRoot;

import java.util.List;

public class PatternRootConfig extends ConfigurableRoot {
    private List<TextItemConfig> sql;
    private List<SchemaConfig> schema;
    private List<String> lib;
    private String schemaName;
    private List<String> initCode;

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
        String table;
        String defaultCode;
        List<TextItemConfig> sql;

        public String getTable() {
            return table;
        }

        public void setTable(String tables) {
            this.table = tables;
        }

        public String getDefaultCode() {
            return defaultCode;
        }

        public void setDefaultCode(String defaultCode) {
            this.defaultCode = defaultCode;
        }

        public List<TextItemConfig> getSql() {
            return sql;
        }

        public void setSql(List<TextItemConfig> sql) {
            this.sql = sql;
        }
    }

    public List<TextItemConfig> getSql() {
        return sql;
    }

    public void setSql(List<TextItemConfig> sql) {
        this.sql = sql;
    }

    public List<SchemaConfig> getSchema() {
        return schema;
    }

    public void setSchema(List<SchemaConfig> schemas) {
        this.schema = schemas;
    }

    public List<String> getLib() {
        return lib;
    }

    public void setLib(List<String> lib) {
        this.lib = lib;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public List<String> getInitCode() {
        return initCode;
    }

    public void setInitCode(List<String> code) {
        this.initCode = code;
    }
}