package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBContext;
import lombok.Data;

import java.util.Collections;
import java.util.Map;

public class SQLContextImpl implements SQLContext {

    private MycatDBClientMediator mycatDBClientMediator;

    public SQLContextImpl(MycatDBClientMediator mycatDBClientMediator) {
        this.mycatDBClientMediator = mycatDBClientMediator;
    }

    @Override
    public MycatDBContext getMycatDBContext() {
        return mycatDBClientMediator;
    }

    @Override
    public Object getSQLVariantRef(String target) {
        return mycatDBClientMediator.getVariable(target);
    }

    @Override
    public Map<String, Object> getParameters() {
        return Collections.emptyMap();
    }


    @Override
    public void setParameters(Map<String, Object> parameters) {

    }


    @Override
    public Map<String, MySQLFunction> functions() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        return mycatDBClientMediator.getSchema();
    }

    @Data
    static class TableCollector extends MySqlASTVisitorAdapter {
        private SQLExprTableSource someTables;

        @Override
        public boolean visit(SQLExprTableSource x) {
            someTables = x;
            return super.visit(x);
        }
    }

//    @Override
//    public SelectStatementHandler selectStatementHandler() {
//        return (statement, receiver) -> {
//            Optional<SQLSelectQueryBlock> sqlSelectQueryBlockMaybe = Optional.ofNullable(statement).map(s -> s.getSelect()).map(s -> s.getQueryBlock());
//            Optional<SQLTableSource> sqlTableSource = sqlSelectQueryBlockMaybe.map(i -> i.getFrom());
//            if (sqlTableSource.isPresent()) {
//                //  有表sql
//                SQLTableSource tableSource = sqlTableSource.get();
//                SQLSelectQueryBlock sqlSelectQueryBlock = sqlSelectQueryBlockMaybe.get();
//                boolean forUpdate = sqlSelectQueryBlock.isForUpdate();
//
//                TableCollector tableCollector = new TableCollector();
//                tableSource.accept(tableCollector);
//                SQLExprTableSource someTables = tableCollector.getSomeTables();
//                if(someTables.getSchema() == null) {
//                    receiver.sendError(new MycatException("unknown schema. sql={}",statement));
//                    return;
//                }
//                if(someTables.getTableName() == null) {
//                    receiver.sendError(new MycatException("unknown tableName. sql={}",statement));
//                    return;
//                }
//                String schemaName = SQLUtils.normalize(someTables.getSchema().toLowerCase());
//                String tableName = SQLUtils.normalize(someTables.getTableName().toLowerCase());
//
//
//                ///////////////////////////////common///////////////////////////////
//                Map<String, SchemaHandler> schemaMap = mycatDBClientMediator.config().getSchemaMap();
//                SchemaHandler schemaHandler = schemaMap.get(schemaName);
//                if (schemaHandler == null) {
//                    String defaultSchema = getDefaultSchema();
//                    if (defaultSchema != null) {
//                        schemaHandler = schemaMap.get(defaultSchema);
//                    } else {
//                        Optional<String> targetNameOptional = Optional.ofNullable(RootHelper.INSTANCE)
//                                .map(i->i.getConfigProvider())
//                                .map(i->i.currentConfig())
//                                .map(i->i.getMetadata())
//                                .map(i->i.getPrototype())
//                                .map(i->i.getTargetName());
//                        if (targetNameOptional.isPresent()){
//                            String targetName = targetNameOptional.get();
//                            receiver.proxySelect(targetName,Objects.toString(statement));
//                            return;
//                        }
//                        String datasource = ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource();
//                        receiver.proxySelect(datasource,Objects.toString(statement));
//                        return;
//                    }
//                }
//                String defaultTargetName = schemaHandler.defaultTargetName();
//                Map<String, TableHandler> tableMap = schemaHandler.logicTables();
//                TableHandler tableHandler = tableMap.get(tableName);
//                ///////////////////////////////common///////////////////////////////
//
//                if (tableHandler == null) {
//                    receiver.proxySelect(defaultTargetName, statement);
//                    return;
//                }
//                MycatDBSharedServer uponDBSharedServer = mycatDBClientMediator.getUponDBSharedServer();
//                MycatSQLPrepareObject mycatSQLPrepareObject = uponDBSharedServer
//                        .innerQueryPrepareObject(statement.toString(), mycatDBClientMediator);
//                PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
//                if (plan instanceof MycatSqlPlanner) {
//                    ProxyInfo proxyInfo = ((MycatSqlPlanner) plan).tryGetProxyInfo();
//                    if (proxyInfo!=null) {
//                        String sql = proxyInfo.getSql();
//                        String targetName = proxyInfo.getTargetName();
//                        boolean updateOpt = proxyInfo.isUpdateOpt();
//                        receiver.proxySelect(targetName, sql);
//                        return;
//                    }
//                }
//                receiver.sendResultSet(plan.run(), () -> plan.explain());
//            } else {
//                receiver.evalSimpleSql(statement);
//            }
//        };
//    }
//
//    @Override
//    public InsertStatementHandler insertStatementHandler() {
//        return new InsertStatementHandler() {
//            @Override
//            public void handleInsert(MySqlInsertStatement statement, Response receiver) {
//                SQLExprTableSource tableSource = statement.getTableSource();
//                updateHandler(statement, tableSource, receiver);
//            }
//
//            @Override
//            public void handleReplace(SQLReplaceStatement statement, Response receiver) {
//                SQLExprTableSource tableSource = statement.getTableSource();
//                updateHandler(statement, tableSource, receiver);
//            }
//        };
//    }
//
//
//
//    @Override
//    public DeleteStatementHandler deleteStatementHandler() {
//        return (statement, receiver) -> {
//            SQLExprTableSource exprTableSource = statement.getExprTableSource();
//            updateHandler(statement, exprTableSource, receiver);
//        };
//    }
//
//    @Override
//    public LoaddataStatementHandler loaddataStatementHandler() {
//        return (statement, receiver) -> receiver.sendError(new MycatException("unsupport loaddata"));
//    }
//
//    @Override
//    public SetStatementHandler setStatementHandler() {
//        return (statement, receiver) -> {
//            List<SQLAssignItem> items = statement.getItems();
//            if (items == null) {
//                items = Collections.emptyList();
//            }
//            for (SQLAssignItem item : items) {
//                String name = Objects.toString(item.getTarget()).toLowerCase();
//                String value = Objects.toString(item.getValue());
//                mycatDBClientMediator.setVariable(name, value);
//            }
//            receiver.sendOk();
//        };
//    }
//
//
//    @Override
//    public TCLStatementHandler tclStatementHandler() {
//        return new TCLStatementHandler() {
//            @Override
//            public void handleSQLStartTransaction(SQLStartTransactionStatement statement, Response receiver) {
//                mycatDBClientMediator.begin();
//                receiver.sendOk();
//            }
//
//            @Override
//            public void handleRollback(SQLRollbackStatement statement, Response receiver) {
//                mycatDBClientMediator.rollback();
//                receiver.sendOk();
//            }
//
//            @Override
//            public void handleCommit(SQLCommitStatement statement, Response receiver) {
//                mycatDBClientMediator.commit();
//                receiver.sendOk();
//            }
//
//            @Override
//            public void handleSetTransaction(MySqlSetTransactionStatement statement, Response receiver) {
//                String isolationLevel = statement.getIsolationLevel();
//                MySQLIsolation mySQLIsolation = MySQLIsolation.parse(isolationLevel);
//                if (mySQLIsolation == null) {
//                    receiver.sendError(new MycatException("非法字符串:" + isolationLevel));
//                    return;
//                }
//                int jdbcValue = mySQLIsolation.getJdbcValue();
//                mycatDBClientMediator.setTransactionIsolation(jdbcValue);
//                receiver.sendOk();
//            }
//        };
//    }

    @Override
    public void setDefaultSchema(String simpleName) {
        if (simpleName != null) {
            mycatDBClientMediator.useSchema(simpleName);
        }
    }

//    @Override
//    public UtilityStatementHandler utilityStatementHandler() {
//        return new UtilityStatementHandler() {
//            @Override
//            public void handleExplain(MySqlExplainStatement statement, Response receiver) {
//                SQLStatement explainStatement = statement.getStatement();
//                String sql = explainStatement.toString();
//                receiver.sendOk();
//            }
//
//            @Override
//            public void handleKill(MySqlKillStatement statement, Response receiver) {
//                receiver.sendOk();
//            }
//
//            @Override
//            public void handleUse(SQLUseStatement statement, Response receiver) {
//                String simpleName = statement.getDatabase().getSimpleName();
//                mycatDBClientMediator.useSchema(simpleName);
//                receiver.sendOk();
//            }
//
//            @Override
//            public void handleSQLShowDatabasesStatement(SQLShowDatabasesStatement statement, Response receiver) {
//                receiver.proxyShow(statement);
//            }
//        };
//    }
//
//    @Override
//    public ReplaceStatementHandler replaceStatementHandler() {
//        return (statement, receiver) -> updateHandler(statement, (SQLExprTableSource) statement.getTableSource(), receiver);
//
//    }
//
//    @Override
//    public DDLStatementHandler ddlStatementHandler() {
//        return new DDLStatementHandlerImpl(mycatDBClientMediator);
//    }
//
//    @Override
//    public ShowStatementHandler showStatementHandler() {
//        return new ShowStatementHandlerImpl(mycatDBClientMediator);
//    }
//
//    @Override
//    public UpdateStatementHandler updateStatementHandler() {
//        return (statement, receiver) -> updateHandler(statement, (SQLExprTableSource) statement.getTableSource(), receiver);
//    }

    @Override
    public long lastInsertId() {
        return this.mycatDBClientMediator.lastInsertId();
    }
//
//    @Override
//    public TruncateStatementHandler truncateStatementHandler() {
//        return (statement, receiver) -> receiver.sendError(new MycatException("unsupport truncate"));
//    }
//
//    @Override
//    public HintStatementHanlder hintStatementHanlder() {
//        return new HintStatementHanlder() {
//            @Override
//            public void handlehintStatement(MySqlHintStatement statement1, Response receiver) {
//                List<SQLStatement> hintStatements = statement1.getHintStatements();
//                if (hintStatements.size()>1){
//                    receiver.sendError(new MycatException("unsupport multi statements"));
//                }else {
//                    SQLHanlder sqlHanlder = new SQLHanlder(SQLContextImpl.this);
//                    sqlHanlder.handleStatement(hintStatements.get(0),receiver);
//                }
//            }
//        };
//    }


}