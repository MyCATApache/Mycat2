/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.client;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.config.PatternRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.pattern.*;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum ClientRuntime {
    INSTANCE;
    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ClientRuntime.class);
    final BuilderInfo wapper = new BuilderInfo();
    volatile RuntimeInfo runtimeInfo;
    TransactionType transactionType = TransactionType.JDBC_TRANSACTION_TYPE;
    private String defaultSchema;

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


    public MycatClient login(MycatDataContext dataContext) {
        return new MycatClient() {
            private RuntimeInfo runtime = Objects.requireNonNull(runtimeInfo);
            private final MycatDBClientMediator db = MycatDBs.createClient(dataContext);

            @Override
            public String transactionType() {
                return dataContext.transactionType();
            }

            @Override
            public TransactionSession getTransactionSession() {
                return dataContext.getTransactionSession();
            }

            @Override
            public void setTransactionSession(TransactionSession transactionSession) {
                dataContext.setTransactionSession(transactionSession);
            }

            @Override
            public void switchTransaction(String transactionSessionType) {
                dataContext.switchTransaction(transactionSessionType);
            }


            @Override
            public <T> T getVariable(MycatDataContextEnum name) {
                return dataContext.getVariable(name);
            }

            @Override
            public void setVariable(MycatDataContextEnum name, Object value) {
                dataContext.setVariable(name, value);
            }

            @Override
            public int serverStatus() {
                return dataContext.serverStatus();
            }

            @Override
            public boolean isAutocommit() {
                return dataContext.isAutocommit();
            }

            @Override
            public void setAutoCommit(boolean autoCommit) {
                dataContext.setAutoCommit(autoCommit);
            }

            @Override
            public MySQLIsolation getIsolation() {
                return dataContext.getIsolation();
            }

            @Override
            public void setIsolation(MySQLIsolation isolation) {
                dataContext.setIsolation(isolation);
            }

            @Override
            public boolean isInTransaction() {
                return dataContext.isInTransaction();
            }

            @Override
            public void setInTransaction(boolean inTransaction) {
                dataContext.setInTransaction(inTransaction);
            }

            @Override
            public MycatUser getUser() {
                return dataContext.getUser();
            }

            @Override
            public void useShcema(String schema) {
                dataContext.useShcema(schema);
            }

            @Override
            public void setUser(MycatUser user) {
                dataContext.setUser(user);
            }
            ///////////////////////////////////////////////

            @Override
            public Context analysis(String sql) {

                RuntimeInfo runtime = this.runtime;
                GPattern tableCollectorPattern = this.runtime.tableCollector.get();

                TableCollector tableMatcher = tableCollectorPattern.getCollector();
                if (dataContext.getDefaultSchema() != null) {
                    tableMatcher.useSchema(dataContext.getDefaultSchema());
                }
                tableCollectorPattern.collect(sql);
                boolean tableMatch = tableMatcher.isMatch();
                Map<String, Collection<String>> collectionMap = tableMatcher.geTableMap();
                if (tableMatch) {
                    for (Map.Entry<String, Collection<String>> stringCollectionEntry : collectionMap.entrySet()) {
                        for (Map.Entry<Map<String, Set<String>>, List<TableInfo>> mapListEntry : this.runtime.tableToItem.entrySet()) {
                            Set<String> tableConfigs = mapListEntry.getKey().get(stringCollectionEntry.getKey());
                            Collection<String> currentTables = stringCollectionEntry.getValue();
                            if (tableConfigs != null && tableConfigs.containsAll(currentTables)) {
                                List<TableInfo> tableInfo = mapListEntry.getValue();
                                if (tableInfo != null) {
                                    for (TableInfo info : tableInfo) {
                                        GPattern pattern = info.pattern.get();
                                        GPatternMatcher matcher = pattern.matcher(sql);
                                        Map<String, String> map = matcher.namesContext();
                                        PatternRootConfig.TextItemConfig textItemConfig = info.map.get(matcher.id());
                                        if (textItemConfig != null) {
                                            String name = textItemConfig.getName();
                                            return getContext(name,
                                                    sql,
                                                    collectionMap,
                                                    map,
                                                    textItemConfig,
                                                    matcher.id());
                                        }
                                        if (info.handler != null) {
                                            String name = Objects.toString(info);
                                            return getContext(name, sql, collectionMap, map, info.handler,null);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                GPattern pattern = runtime.noTablesPattern.get();
                GPatternMatcher matcher = pattern.matcher(sql);
                if (matcher.acceptAll()) {
                    PatternRootConfig.TextItemConfig textItemConfig = runtime.idToItem.get(matcher.id());
                    if (textItemConfig != null) {
                        String name = Objects.toString(textItemConfig);
                        return getContext(name, sql, collectionMap, matcher.namesContext(), textItemConfig,matcher.id());
                    }
                }
                if (runtimeInfo.defaultHandler != null) {
                    String name = "defaultHandler";
                    return getContext(name, sql, collectionMap, matcher.namesContext(), runtimeInfo.defaultHandler,null);
                }
                throw new UnsupportedOperationException();
            }

            private Context getContext(String name, String sql, Map<String, Collection<String>> geTableMap, Map<String, String> namesContext, PatternRootConfig.Handler handler,Integer sqlId) {
                return new Context(name, sql, geTableMap, namesContext, handler.getTags(), handler.getHints(), handler.getCommand(), handler.getExplain(), sqlId,handler.getCache()!=null,handler.getSimply());
            }


            @NotNull
            private Context getContext(String name,
                                       String sql,
                                       Map<String, Collection<String>> geTableMap,
                                       Map<String, String> namesContext,
                                       PatternRootConfig.TextItemConfig handler,
                                       Integer sqlId) {
                return new Context(name, sql, geTableMap, namesContext, handler.getTags(), handler.getHints(), handler.getCommand(), handler.getExplain(), sqlId,handler.getCache()!=null, handler.getSimply());
            }

            @Override
            public void useSchema(String schemaName) {
                if (schemaName != null) {
                    db.useSchema(schemaName);
                    dataContext.useShcema(schemaName);
                } else {
                    LOGGER.warn("use null schema");
                }
            }

            @Override
            public TransactionType getTransactionType() {
                TransactionType parse = TransactionType.parse(dataContext.getTransactionSession().name());
                return parse;
            }

            @Override
            public void useTransactionType(TransactionType transactionType) {
                dataContext.switchTransaction(transactionType.getName());
            }

            @Override
            public String getDefaultSchema() {
                return dataContext.getDefaultSchema();
            }

            @Override
            public int getServerCapabilities() {
                return dataContext.getServerCapabilities();
            }

            @Override
            public int getWarningCount() {
                return dataContext.getWarningCount();
            }

            @Override
            public long getLastInsertId() {
                return dataContext.getLastInsertId();
            }

            @Override
            public Charset getCharset() {
                return dataContext.getCharset();
            }

            @Override
            public int getCharsetIndex() {
                return dataContext.getCharsetIndex();
            }

            @Override
            public void setLastInsertId(long s) {
                dataContext.setLastInsertId(s);
            }

            @Override
            public int getLastErrorCode() {
                return dataContext.getLastErrorCode();
            }

            @Override
            public long getAffectedRows() {
                return dataContext.getAffectedRows();
            }

            @Override
            public void setLastMessage(String lastMessage) {
                dataContext.setLastMessage(lastMessage);
            }

            @Override
            public String getLastMessage() {
                return dataContext.getLastMessage();
            }

            @Override
            public void setServerCapabilities(int serverCapabilities) {
                dataContext.setServerCapabilities(serverCapabilities);
            }

            @Override
            public void setAffectedRows(long affectedRows) {
                dataContext.setAffectedRows(affectedRows);
            }

            @Override
            public void setCharset(int index, String charsetName, Charset defaultCharset) {
                dataContext.setCharset(index, charsetName, defaultCharset);
            }

            @Override
            public AtomicBoolean getCancelFlag() {
                return dataContext.getCancelFlag();
            }

            @Override
            public boolean isRunning() {
                return dataContext.isRunning();
            }

            @Override
            public int getNumParamsByStatementId(long statementId) {
                return dataContext.getNumParamsByStatementId(statementId);
            }

            @Override
            public void run(Runnable runnable) {
                dataContext.run(runnable);
            }

            @Override
            public boolean isReadOnly() {
                return dataContext.isReadOnly();
            }

            @Override
            public UpdateRowIteratorResponse update(String targetName, String sql) {
                return dataContext.update(targetName, sql);
            }

            @Override
            public RowBaseIterator query(String targetName, String sql) {
                return dataContext.query(targetName, sql);
            }

            @Override
            public RowBaseIterator queryDefaultTarget(String sql) {
                return dataContext.queryDefaultTarget(sql);
            }

            @Override
            public boolean continueBindThreadIfTransactionNeed() {
                return dataContext.continueBindThreadIfTransactionNeed();
            }

            @Override
            public void close() {
                dataContext.close();
            }

            @Override
            public void block(Runnable runnable) {
                dataContext.run(runnable);
            }

            @Override
            public String resolveDatasourceTargetName(String targetName) {
                return dataContext.resolveDatasourceTargetName(targetName);
            }

            @Override
            public <T> T unwrap(Class<T> iface) throws Exception {
                return dataContext.unwrap(iface);
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws Exception {
                return dataContext.isWrapperFor(iface);
            }

            @Override
            public MycatDBClientMediator getMycatDb() {
                return db;
            }
        };
    }


    public void flash() {
        //config
        final PatternRootConfig patternRootConfig = wapper.patternRootConfig;
        List<PatternRootConfig.TextItemConfig> sqls = new ArrayList<>(patternRootConfig.getSqls());
        List<PatternRootConfig.SchemaConfig> schemas = new ArrayList<>(patternRootConfig.getSchemas());
        PatternRootConfig.Handler defaultHanlder = patternRootConfig.getDefaultHanlder();
        //builder

        //pre
//        for (PatternRootConfig.HandlerToSQLs handler : patternRootConfig.getHandlers()) {
//            String name = handler.getName();
//            String explainSql = handler.getExplain();
//            Map<String, String> tags = handler.getTags();
//            String type = handler.getType();
//
//            List<String> tables = handler.getTables();
//            if (tables.isEmpty()) {
//                for (String sql : handler.getSqls()) {
//                    PatternRootConfig.TextItemConfig textItemConfig = new PatternRootConfig.TextItemConfig();
//                    textItemConfig.setName(name);
//                    textItemConfig.setSql(sql);
//                    textItemConfig.setTags(tags);
//                    textItemConfig.setExplain(explainSql);
//                    textItemConfig.setCommand(type);
//                    sqls.add(textItemConfig);
//                }
//            } else {
//                ArrayList<PatternRootConfig.TextItemConfig> textItemConfigs = new ArrayList<PatternRootConfig.TextItemConfig>();
//                for (String sql : handler.getSqls()) {
//                    PatternRootConfig.TextItemConfig textItemConfig = new PatternRootConfig.TextItemConfig();
//                    textItemConfig.setName(name);
//                    textItemConfig.setSql(sql);
//                    textItemConfig.setTags(tags);
//                    textItemConfig.setExplain(explainSql);
//                    textItemConfigs.add(textItemConfig);
//                    textItemConfig.setCommand(type);
//                }
//                PatternRootConfig.SchemaConfig schemaConfig = new PatternRootConfig.SchemaConfig();
//                schemaConfig.setDefaultHanlder(null);
//                schemaConfig.setName(name);
//                schemaConfig.setTables(tables);
//                schemaConfig.setSqls(textItemConfigs);
//                schemas.add(schemaConfig);
//            }
//        }
        //build
        ConcurrentHashMap<Integer, PatternRootConfig.TextItemConfig> itemMap = new ConcurrentHashMap<>();
        final GPatternBuilder noTablesPatternBuilder = new GPatternBuilder(0);
        for (PatternRootConfig.TextItemConfig textItemConfig : sqls) {
            itemMap.put(noTablesPatternBuilder.addRule(textItemConfig.getSql()), textItemConfig);
        }
        Supplier<GPattern> noTablesPattern = () -> noTablesPatternBuilder.createGroupPattern();


        Map<Map<String, Set<String>>, List<TableInfo>> tableMap = new ConcurrentHashMap<>();
        for (PatternRootConfig.SchemaConfig schema : schemas) {
            ConcurrentHashMap<Integer, PatternRootConfig.TextItemConfig> map = new ConcurrentHashMap<>();
            final GPatternBuilder tablesPatternBuilder = new GPatternBuilder(0);
            for (PatternRootConfig.TextItemConfig sql : schema.getSqls()) {
                map.put(tablesPatternBuilder.addRule(sql.getSql()), sql);
            }
            List<TableInfo> tableInfos1 = tableMap.computeIfAbsent(getTableMap(schema), stringSetMap -> new CopyOnWriteArrayList<>());
            tableInfos1.add(new TableInfo(map, schema.getDefaultHanlder(), () -> tablesPatternBuilder.createGroupPattern()));
        }
        GPatternIdRecorderImpl gPatternIdRecorder = new GPatternIdRecorderImpl(false);
        TableCollectorBuilder tableCollectorBuilder = new TableCollectorBuilder(gPatternIdRecorder, (Map) getTableMap(schemas));
        final GPatternBuilder tableCollectorPatternBuilder = new GPatternBuilder(0);
        runtimeInfo = new RuntimeInfo(() -> tableCollectorPatternBuilder.createGroupPattern(tableCollectorBuilder.create()), itemMap, tableMap, defaultHanlder, noTablesPattern);
        this.transactionType = TransactionType.parse(patternRootConfig.getTransactionType());
        this.defaultSchema = patternRootConfig.getDefaultSchema();
    }

    private Map<String, Set<String>> getTableMap(List<PatternRootConfig.SchemaConfig> schemaConfigs) {
        return schemaConfigs.stream().flatMap(i -> {
            return i.getTables().stream().map(commonTableName -> apply(commonTableName));
        }).collect(Collectors.groupingBy(k -> k.getSchemaName(), Collectors.mapping(v -> v.getTableName(), Collectors.toSet())));
    }

    private Map<String, Set<String>> getTableMap(PatternRootConfig.SchemaConfig schemaConfig) {
        return schemaConfig.getTables().stream().map(ClientRuntime::apply).collect(Collectors.groupingBy(k -> k.getSchemaName(), Collectors.mapping(v -> v.getTableName(), Collectors.toSet())));

    }


    public static void main(String[] args) {

    }

    public void load(MycatConfig config) {
        wapper.setPatternRootConfig(config.getInterceptor());
        flash();
    }


    @Getter
    static class TableInfo {
        final Map<Integer, PatternRootConfig.TextItemConfig> map;
        final PatternRootConfig.Handler handler;
        final Supplier<GPattern> pattern;

        public TableInfo(Map<Integer, PatternRootConfig.TextItemConfig> map, PatternRootConfig.Handler handler, Supplier<GPattern> pattern) {
            this.map = map;
            this.handler = handler;
            this.pattern = pattern;
        }
    }


    private static class RuntimeInfo {
        final Supplier<GPattern> tableCollector;
        final Map<Integer, PatternRootConfig.TextItemConfig> idToItem;
        final Map<Map<String, Set<String>>, List<TableInfo>> tableToItem;
        final PatternRootConfig.Handler defaultHandler;
        final Supplier<GPattern> noTablesPattern;

        public RuntimeInfo(Supplier<GPattern> tableCollectorBuilder, Map<Integer, PatternRootConfig.TextItemConfig> idToItem,
                           Map<Map<String, Set<String>>, List<TableInfo>> tableToItem,
                           PatternRootConfig.Handler defaultHandler, Supplier<GPattern> noTablesPattern) {
            this.tableCollector = tableCollectorBuilder;
            this.idToItem = idToItem;
            this.tableToItem = tableToItem;
            this.defaultHandler = defaultHandler;
            this.noTablesPattern = noTablesPattern;
        }
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

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }}