package io.mycat.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class PatternRootConfig {
    private List<TextItemConfig> sql = new ArrayList<>();
    private List<SchemaConfig> schema = new ArrayList<>();
    private List<String> lib = new ArrayList<>();
    private String schemaName = "TESTDB";
    private List<String> initCode = new ArrayList<>();
    private String defaultCode = "UNKNOWN";

    @Data
    public static class TextItemConfig {
        String sql;
        String code;
    }

    @Data
    public static class SchemaConfig {
        String table;
        String defaultCode;
        List<TextItemConfig> sql;
    }

}