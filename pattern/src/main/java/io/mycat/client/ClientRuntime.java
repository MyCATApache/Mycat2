package io.mycat.client;

import io.mycat.EvalNodeVisitor;
import io.mycat.MycatConfig;
import io.mycat.config.PatternRootConfig;
import io.mycat.pattern.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.eclipse.collections.api.block.function.Function2;
import org.jetbrains.annotations.NotNull;
import org.reflections.Reflections;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static javax.swing.UIManager.get;

public enum ClientRuntime {
    INSTANCE;

    final BuilderInfo wapper = new BuilderInfo();
    final ConcurrentHashMap<String, List<EvalNodeVisitor.FunctionSig>> libSharedMap = new ConcurrentHashMap<>();
    volatile RuntimeInfo runtimeInfo;
    String transactionType = "jdbc";

    private static SchemaTable apply(String commonTableName) {
        String[] split1 = commonTableName.split("\\.");
        String schemaName = split1[0].intern();
        String tableName = split1[1].intern();
        return new SchemaTable(schemaName, tableName);
    }

    public synchronized void addDefaultHanlder(PatternRootConfig.Handler handler) {
        wapper.replaceDefaultHanlder(handler);
    }

    public synchronized void addOrReplaceSQLMatch(PatternRootConfig.TextItemConfig textItemConfig) {
        wapper.addOrReplaceSQLMatch(textItemConfig);
    }

    public synchronized void removeSQLMatch(String name) {
        wapper.removeSQLMatch(name);
    }

    public synchronized void addOrReplaceSchemaMatch(PatternRootConfig.SchemaConfig schemaConfig) {
        wapper.addOrReplaceSchemaMatch(schemaConfig);
    }

    public synchronized void removeSchemaMatch(String name) {
        wapper.removeSchemaMatch(name);
    }


    public MycatClient login(String username, String password) {
        return new MycatClient() {
            private RuntimeInfo runtime = Objects.requireNonNull(runtimeInfo);
            private GPattern pattern = runtime.supplier.get();
            private String defaultSchemaName;
            private String transactionType;
            ///////////////////////////////////////////////

            @Override
            public Context analysis(String sql) {

                @NonNull GPattern currentPattern = getCurrentPattern();
                RuntimeInfo runtime = this.runtime;
                TableCollector tableMatcher = currentPattern.getCollector();
                if (defaultSchemaName!=null) {
                    tableMatcher.useSchema(defaultSchemaName);
                }
                GPatternMatcher matcher = currentPattern.matcherAndCollect(sql);
                boolean sqlMatch = matcher.acceptAll();
                boolean tableMatch = tableMatcher.isMatch();

                Map<String, Collection<String>> collectionMap = tableMatcher.geTableMap();
                Map<String, String> map = matcher.namesContext();
                if (sqlMatch && tableMatch) {

                    for (Map.Entry<String, Collection<String>> stringCollectionEntry : collectionMap.entrySet()) {
                        for (Map.Entry<Map<String, Set<String>>, List<TableInfo>> mapListEntry : this.runtime.tableToItem.entrySet()) {
                            Set<String> strings = mapListEntry.getKey().get(stringCollectionEntry.getKey());
                            if(strings.containsAll(stringCollectionEntry.getValue())){
                                List<TableInfo> tableInfo = mapListEntry.getValue();
                                if (tableInfo != null) {
                                    for (TableInfo info : tableInfo) {
                                        PatternRootConfig.TextItemConfig textItemConfig =info .map.get(matcher.id());
                                        if (textItemConfig != null) {
                                            return getContext(sql,collectionMap,map,textItemConfig);
                                        }
                                        if (info.handler != null) {
                                            return getContext(sql,collectionMap,map,info.handler);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (sqlMatch && !tableMatch) {
                    Map<Integer, PatternRootConfig.TextItemConfig> idToItem = runtime.idToItem;
                    PatternRootConfig.TextItemConfig textItemConfig = idToItem.get(matcher.id());
                    if (textItemConfig != null) {
                        return getContext(sql,collectionMap,map,textItemConfig);
                    }
                }

                if (!sqlMatch && tableMatch) {
                    List<TableInfo> tableInfo = this.runtime.tableToItem.get(tableMatcher.geTableMap());
                    if (tableInfo != null&&tableInfo.size()==1) {
                        if (tableInfo.get(0).handler != null)
                            return getContext(sql,collectionMap,map,tableInfo.get(0).handler);
                    }
                }
                if (!sqlMatch && !tableMatch) {
                    return getContext(sql,collectionMap,map,runtime.defaultHandler);
                }
                if (runtimeInfo.defaultHandler!=null){
                 return    getContext(sql,collectionMap,map,runtimeInfo.defaultHandler);
                }
                throw new UnsupportedOperationException();
            }

            private Context getContext(String sql,Map<String, Collection<String>> geTableMap, Map<String, String> namesContext, PatternRootConfig.Handler handler) {
                return new Context(sql,geTableMap,namesContext,handler.getTags(),handler.getType(),handler.getExplain(),handler.getTransactionType());
            }


            @NotNull
            private Context getContext(String sql,Map<String, Collection<String>> geTableMap, Map<String, String> namesContext, PatternRootConfig.TextItemConfig handler) {
                return new Context(sql,geTableMap,namesContext,handler.getTags(),handler.getType(),handler.getExplain(),handler.getTransactionType());
            }

            @Override
            public List<String> explain(String sql) {
                return null;
            }

            @Override
            public void useSchema(String schemaName) {
                this.defaultSchemaName = schemaName;
            }

            @Override
            public String getTransactionType() {
                return transactionType;
            }
            @Override
            public void useTransactionType(String transactionType){
                this.transactionType = transactionType;
            }

            @Override
            public String getDefaultSchema() {
                return defaultSchemaName;
            }

            @NonNull
            private GPattern getCurrentPattern() {
                RuntimeInfo newSourceSupplier = Objects.requireNonNull(ClientRuntime.INSTANCE.runtimeInfo);
                if (this.runtime != newSourceSupplier) {
                    this.runtime = newSourceSupplier;
                    this.pattern = Objects.requireNonNull((GPattern) newSourceSupplier.supplier.get());
                }
                return pattern;
            }
        };
    }


    public void flash() {
        //config
        final PatternRootConfig patternRootConfig = wapper.patternRootConfig;
        List<PatternRootConfig.TextItemConfig> sqls = new ArrayList<>( patternRootConfig.getSqls());
        List<PatternRootConfig.SchemaConfig> schemas = new ArrayList<>(patternRootConfig.getSchemas());
        PatternRootConfig.Handler defaultHanlder = patternRootConfig.getDefaultHanlder();
        //builder
        final GPatternBuilder patternBuilder = new GPatternBuilder(0);
        //pre
        for (PatternRootConfig.HandlerToSQLs handler : patternRootConfig.getHandlers()) {
            String name = handler.getName();
            String explain = handler.getExplain();
            Map<String, String> tags = handler.getTags();
            String type = handler.getType();

            List<String> tables = handler.getTables();
            if (tables.isEmpty()){
                for (String sql : handler.getSqls()) {
                    PatternRootConfig.TextItemConfig textItemConfig = new PatternRootConfig.TextItemConfig();
                    textItemConfig.setName(name);
                    textItemConfig.setSql(sql);
                    textItemConfig.setTags(tags);
                    textItemConfig.setExplain(explain);
                    textItemConfig.setType(type);
                    sqls.add(textItemConfig);
                }
            }else {
                ArrayList<PatternRootConfig.TextItemConfig> textItemConfigs = new ArrayList<PatternRootConfig.TextItemConfig>();
                for (String sql : handler.getSqls()) {
                    PatternRootConfig.TextItemConfig textItemConfig = new PatternRootConfig.TextItemConfig();
                    textItemConfig.setName(name);
                    textItemConfig.setSql(sql);
                    textItemConfig.setTags(tags);
                    textItemConfig.setExplain(explain);
                    textItemConfigs.add(textItemConfig);
                    textItemConfig.setType(type);
                }
                PatternRootConfig.SchemaConfig schemaConfig = new PatternRootConfig.SchemaConfig();
                schemaConfig.setDefaultHanlder(null);
                schemaConfig.setName(name);
                schemaConfig.setTables(tables);
                schemaConfig.setSqls(textItemConfigs);
                schemas.add(schemaConfig);
            }
        }
        //build
        HashMap<Integer, PatternRootConfig.TextItemConfig> itemMap = new HashMap<>();
        for (PatternRootConfig.TextItemConfig textItemConfig : sqls) {
            itemMap.put(patternBuilder.addRule(textItemConfig.getSql()), textItemConfig);
        }
        Map<Map<String, Set<String>>, List<TableInfo>> tableMap = new HashMap<>();
        for (PatternRootConfig.SchemaConfig schema : schemas) {
            HashMap<Integer, PatternRootConfig.TextItemConfig> map = new HashMap<>();
            for (PatternRootConfig.TextItemConfig sql : schema.getSqls()) {
                map.put(patternBuilder.addRule(sql.getSql()), sql);
            }
            List<TableInfo> tableInfos1 = tableMap.computeIfAbsent(getTableMap(schema), stringSetMap -> new ArrayList<>());
            tableInfos1.add(new TableInfo(map, schema.getDefaultHanlder()));
        }



        TableCollectorBuilder tableCollctorbuilder = new TableCollectorBuilder(patternBuilder.geIdRecorder(), (Map) getTableMap(schemas));
        runtimeInfo = new RuntimeInfo(() -> patternBuilder.createGroupPattern(tableCollctorbuilder.create()), itemMap, tableMap, defaultHanlder);
        this.transactionType = patternRootConfig.getTransactionType();
    }

    private Map<String, Set<String>> getTableMap(List<PatternRootConfig.SchemaConfig> schemaConfigs) {
        return schemaConfigs.stream().flatMap(i -> { return i.getTables().stream().map(commonTableName -> apply(commonTableName));
        }).collect(Collectors.groupingBy(k -> k.getSchemaName(), Collectors.mapping(v -> v.getTableName(), Collectors.toSet())));
    }

    private Map<String, Set<String>> getTableMap(PatternRootConfig.SchemaConfig schemaConfig) {
        return schemaConfig.getTables().stream().map(ClientRuntime::apply).collect(Collectors.groupingBy(k -> k.getSchemaName(), Collectors.mapping(v -> v.getTableName(), Collectors.toSet())));

    }

    public void loadPackageList(List<String> packageNameList) throws IllegalAccessException {
        Reflections reflections = new Reflections(packageNameList);
        Set<Class<? extends InstructionSet>> subTypesOf = reflections.getSubTypesOf(InstructionSet.class);
        if (subTypesOf == null) subTypesOf = Collections.emptySet();
        Map<String, List<EvalNodeVisitor.FunctionSig>> map = EvalNodeVisitor.getMap((Set) subTypesOf);

        for (Map.Entry<String, List<EvalNodeVisitor.FunctionSig>> stringListEntry : map.entrySet()) {
            libSharedMap.put(stringListEntry.getKey(), stringListEntry.getValue());
        }
    }

    public void clearPackage() {
        libSharedMap.clear();
    }


    public static void main(String[] args) {

    }

    public void load(MycatConfig config) {
        wapper.setPatternRootConfig(config.getInterceptor());
        flash();
    }

    @AllArgsConstructor
    @Getter
    static class TableInfo {
        final Map<Integer, PatternRootConfig.TextItemConfig> map;
        final PatternRootConfig.Handler handler;
    }

    @AllArgsConstructor
    private static class RuntimeInfo {
        final Supplier<GPattern> supplier;
        final Map<Integer, PatternRootConfig.TextItemConfig> idToItem;
        final Map<Map<String, Set<String>>, List<TableInfo>> tableToItem;
        final PatternRootConfig.Handler defaultHandler;
    }

    private static class BuilderInfo {
        volatile PatternRootConfig patternRootConfig = new PatternRootConfig();

        public void setPatternRootConfig(PatternRootConfig patternRootConfig) {
            this.patternRootConfig = patternRootConfig;
        }

        public synchronized void replaceDefaultHanlder(PatternRootConfig.Handler handler) {
            Objects.requireNonNull(handler);
            patternRootConfig.setDefaultHanlder(handler);
        }

        public synchronized void addOrReplaceSQLMatch(PatternRootConfig.TextItemConfig textItemConfig) {
            removeSQLMatch(textItemConfig.getName());
            patternRootConfig.getSqls().add(textItemConfig);
        }

        public synchronized void removeSQLMatch(String name) {
            List<PatternRootConfig.TextItemConfig> sqls = patternRootConfig.getSqls();
            Optional<PatternRootConfig.TextItemConfig> first = sqls.stream().filter(i -> name.equals(i.getName())).findFirst();
            first.ifPresent(sqls::remove);
        }

        public synchronized void addOrReplaceSchemaMatch(PatternRootConfig.SchemaConfig schemaConfig) {
            removeSchemaMatch(schemaConfig.getName());
            List<PatternRootConfig.SchemaConfig> schemas = patternRootConfig.getSchemas();
            schemas.add(schemaConfig);
        }

        public synchronized void removeSchemaMatch(String name) {
            List<PatternRootConfig.SchemaConfig> schemas = patternRootConfig.getSchemas();
            Optional<PatternRootConfig.SchemaConfig> first = schemas.stream().filter(i -> name.equals(i.getName())).findFirst();
            first.ifPresent(schemas::remove);
        }
    }

    public String getTransactionType() {
        return transactionType;
    }
}