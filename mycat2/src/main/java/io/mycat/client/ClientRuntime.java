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

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.DrdsRecoverDDLJob;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.config.PatternRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.pattern.*;
import io.mycat.proxy.session.SimpleTransactionSessionRunner;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum ClientRuntime {
    INSTANCE;
    private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ClientRuntime.class);
    final Map<String, BuilderInfo> wapper = new ConcurrentHashMap<>();
    volatile MycatConfig mycatConfig;


    private static SchemaTable splitSchemaTable(String commonTableName) {
        String[] split1 = commonTableName.split("\\.");
        String schemaName = split1[0].intern();
        String tableName = split1[1].intern();
        return new SchemaTable(schemaName, tableName);
    }

    public List<MycatClient> getDefaultUsers() {
        List<MycatClient> list = new ArrayList<>();
        for (Map.Entry<String, BuilderInfo> stringBuilderInfoEntry : wapper.entrySet()) {
            String key = stringBuilderInfoEntry.getKey();
            MycatDataContextImpl mycatDataContext = new MycatDataContextImpl(new SimpleTransactionSessionRunner());
            mycatDataContext.setUser(MycatUser.builder().userName(key).build());
            list.add(login(mycatDataContext, false));
        }
        return list;
    }

    public MycatClient login(MycatDataContext dataContext, boolean check) {
        String userName = dataContext.getUser().getUserName();
        BuilderInfo builderInfo = wapper.get(userName);
        MycatClient client = new MycatClient() {

            private RuntimeInfo runtime = Objects.requireNonNull(builderInfo.runtimeInfo);
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

//                TableCollector tableMatcher = tableCollectorPattern.getCollector();
//                if (dataContext.getDefaultSchema() != null) {
//                    tableMatcher.useSchema(dataContext.getDefaultSchema());
//                }
//                tableCollectorPattern.collect(sql);
                Map<String, Collection<String>> collectionMap = new HashMap<>();
                try {
                    SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
                    sqlStatement.accept(new MySqlASTVisitorAdapter() {
                        @Override
                        public boolean visit(SQLExprTableSource x) {
                            String schema = x.getSchema();
                            String tableName = x.getTableName();
                            if (schema == null) {
                                schema = dataContext.getDefaultSchema();
                            }
                            if (schema == null) {
                                throw new UnsupportedOperationException("please use schema");
                            }
                            schema = SQLUtils.normalize(schema.toLowerCase());
                            tableName = SQLUtils.normalize(tableName.toLowerCase());
                            Collection<String> strings = collectionMap.computeIfAbsent(schema, s -> new HashSet<>());
                            strings.add(tableName);
                            return super.visit(x);
                        }
                    });
                }catch (Throwable t){

                }
                if (!collectionMap.isEmpty()) {
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
                                            return getContext(name, sql, collectionMap, map, info.handler, null);
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
                        return getContext(name, sql, collectionMap, matcher.namesContext(), textItemConfig, matcher.id());
                    }
                }
                if (runtime.defaultHandler != null) {
                    String name = "defaultHandler";
                    return getContext(name, sql, collectionMap, matcher.namesContext(), runtime.defaultHandler, null);
                }
                throw new UnsupportedOperationException();
            }

            private Context getContext(String name, String sql, Map<String, Collection<String>> geTableMap, Map<String, String> namesContext, PatternRootConfig.Handler handler, Integer sqlId) {
                return new Context(name, sql, geTableMap, namesContext, handler.getTags(), handler.getHints(), handler.getCommand(), handler.getExplain(), sqlId, handler.getCache() != null, handler.getSimply());
            }


            @NotNull
            private Context getContext(String name,
                                       String sql,
                                       Map<String, Collection<String>> geTableMap,
                                       Map<String, String> namesContext,
                                       PatternRootConfig.TextItemConfig handler,
                                       Integer sqlId) {
                return new Context(name, sql, geTableMap, namesContext, handler.getTags(), handler.getHints(), handler.getCommand(), handler.getExplain(), sqlId, handler.getCache() != null, handler.getSimply());
            }

            @Override
            public void useSchema(String schemaName) {
                if (schemaName != null) {
                    dataContext.useShcema(schemaName);
                } else {
                    LOGGER.warn("use null schema");
                }
            }

            @Override
            public TransactionType getTransactionType() {
                return TransactionType.parse(dataContext.getTransactionSession().name());
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
            public RowBaseIterator query(MycatRowMetaData mycatRowMetaData, String targetName, String sql) {
                return dataContext.query(mycatRowMetaData, targetName, sql);
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
            public RowBaseIterator query(String targetName, String sql) {
                return dataContext.query(targetName,sql);
            }

            @Override
            public MycatRowMetaData queryMetaData(String targetName, String sql) {
                return dataContext.queryMetaData(targetName,sql);
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
        client.useTransactionType(builderInfo.transactionType);
        return client;
    }


    private synchronized void flash() {
        //config
        this.wapper.clear();
        for (PatternRootConfig interceptor : Objects.requireNonNull(this.mycatConfig).getInterceptors()) {
            PatternRootConfig.UserConfig user = Objects.requireNonNull(interceptor.getUser());
            String username = user.getUsername();

            List<PatternRootConfig.TextItemConfig> sqls = new ArrayList<>(interceptor.getSqls());
            List<PatternRootConfig.SchemaConfig> schemas = new ArrayList<>(interceptor.getSchemas());
            PatternRootConfig.Handler defaultHanlder = interceptor.getDefaultHanlder();
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
            RuntimeInfo runtimeInfo = new RuntimeInfo(() -> tableCollectorPatternBuilder.createGroupPattern(tableCollectorBuilder.create()), itemMap, tableMap, defaultHanlder, noTablesPattern);
            TransactionType transactionType = TransactionType.parse(interceptor.getTransactionType());
            this.wapper.put(username, new BuilderInfo(interceptor, runtimeInfo, transactionType));
        }
    }

    private Map<String, Set<String>> getTableMap(List<PatternRootConfig.SchemaConfig> schemaConfigs) {
        return schemaConfigs.stream().flatMap(i -> {
            return i.getTables().stream().map(commonTableName -> splitSchemaTable(commonTableName));
        }).collect(Collectors.groupingBy(k -> k.getSchemaName(), Collectors.mapping(v -> v.getTableName(), Collectors.toSet())));
    }

    private Map<String, Set<String>> getTableMap(PatternRootConfig.SchemaConfig schemaConfig) {
        return schemaConfig.getTables().stream().map(ClientRuntime::splitSchemaTable).collect(Collectors.groupingBy(k -> k.getSchemaName(), Collectors.mapping(v -> v.getTableName(), Collectors.toSet())));

    }


    public static void main(String[] args) {

    }

    public synchronized void load(MycatConfig config) {
        if (this.mycatConfig == config) {
            return;
        } else {
            this.mycatConfig = config;
        }
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


    public static class BuilderInfo {
        final PatternRootConfig patternRootConfig;
        final RuntimeInfo runtimeInfo;
        final TransactionType transactionType;

        public BuilderInfo(PatternRootConfig patternRootConfig, RuntimeInfo runtimeInfo, TransactionType transactionType) {
            this.patternRootConfig = patternRootConfig;
            this.runtimeInfo = runtimeInfo;
            this.transactionType = transactionType;
        }
    }
}