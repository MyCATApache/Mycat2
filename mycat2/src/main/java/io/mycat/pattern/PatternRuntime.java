//package io.mycat.pattern;
//
//import io.mycat.ConfigRuntime;
//import io.mycat.config.PatternRootConfig;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.Objects;
//
//public enum PatternRuntime {
//    INSTANCE;
//    volatile DynamicSQLMatcherBuilder builder;
//    private Instruction defaultInstruction;
//
//    public void load() throws Exception {
//        load(false);
//    }
//    public void load(boolean force) throws Exception {
//        if (builder == null || force) {
//            DynamicSQLMatcherBuilder builder = new DynamicSQLMatcherBuilder(null);
//            PatternRootConfig patternRootConfig = Objects.requireNonNull(ConfigRuntime.INSTCANE.getConfig(ConfigFile.PATTERN),
//                    "pattern config can not found");
//            List<PatternRootConfig.TextItemConfig> sqlList = patternRootConfig.getSql();
//            if (sqlList != null) {
//                for (PatternRootConfig.TextItemConfig textItemConfig : sqlList) {
//                    builder.add(Objects.requireNonNull(textItemConfig.getSql(), "sql should not be empty"),
//                            Objects.requireNonNull(textItemConfig.getCode(), "code should not be empty"));
//                }
//            }
//            List<PatternRootConfig.SchemaConfig> schema = patternRootConfig.getSchema();
//            if (schema != null) {
//                for (PatternRootConfig.SchemaConfig schemaConfig : schema) {
//                    String table = Objects.requireNonNull(schemaConfig.getTable(), "table should not be empty");
//                    sqlList = schemaConfig.getSql() == null ? Collections.emptyList() : schemaConfig.getSql();
//                    for (PatternRootConfig.TextItemConfig textItemConfig : sqlList) {
//                        builder.addSchema(table, Objects.requireNonNull(textItemConfig.getSql(), "sql should not be empty"),
//                                Objects.requireNonNull(textItemConfig.getCode(), "code should not be empty"));
//                    }
//                    String defaultCode = schemaConfig.getDefaultCode();
//                    if (defaultCode!=null) {
//                        builder.addSchema(table, null, defaultCode);
//                    }
//                }
//            }
//            List<String> lib = patternRootConfig.getLib();
//            if (lib == null) lib = Collections.emptyList();
//
//            String schemaName = patternRootConfig.getSchemaName();
//            builder.build("io.mycat.proxy.session.MycatSession",patternRootConfig.getInitCode()==null?Collections.emptyList():patternRootConfig.getInitCode(),lib,schemaName,patternRootConfig.getDefaultCode(), false);
//            this.builder = builder;
//            this.defaultInstruction = this.builder.getDefaultInstruction();
//        }
//    }
//    public DynamicSQLMatcher createMatcher(){
//        return builder.createMatcher();
//    }
//
//    public Instruction getDefaultInstruction() {
//        return defaultInstruction;
//    }
//}