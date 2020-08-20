package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.interpreter.TypeCalculation;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.optimizer.rules.TableSourceExtractor;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.booster.BoosterRuntime;
import io.mycat.calcite.prepare.MycatSQLPrepareObject;
import io.mycat.calcite.prepare.MycatSqlPlanner;
import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.hbt.HBTRunners;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.metadata.SchemaHandler;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.route.HBTQueryConvertor2;
import io.mycat.route.InputHandler;
import io.mycat.route.ParseContext;
import io.mycat.route.ResultHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.MycatDBSharedServer;
import io.mycat.upondb.ProxyInfo;
import io.mycat.util.Response;
import io.mycat.util.SQLContext;
import lombok.Getter;

import java.sql.JDBCType;
import java.util.*;
import java.util.function.Predicate;

public class SelectSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    //    public static String NULL = new String(new char[]{(char)0XFB});
//    public static int NULL = 0XFB;
    public static String NULL = "NULL";

    public SelectSQLHandler() {
    }

    public SelectSQLHandler(Class statementClass) {
        super(statementClass);
    }

    protected ExecuteCode onSelectNoTable(SQLRequest<SQLSelectStatement> request, Response receiver) {
        return onSelectDual(request, receiver);
    }

    /**
     * impl example
     * select @@last_insert_id, max(1+1),1+2 as b ,'' as b, '3' as c , null as d from dual;
     *
     * @param request
     * @param receiver
     * @return
     */
    protected ExecuteCode onSelectDual(SQLRequest<SQLSelectStatement> request, Response receiver) {
        String sql = request.getAst().toString();
        try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(ReplicaSelectorRuntime.INSTANCE.getPrototypeOrFirstReplicaDataSource())) {
            try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql)) {
                receiver.sendResultSet(() -> rowBaseIterator, () -> Arrays.asList(sql));
            }
        }   return ExecuteCode.PERFORMED;
    }

    protected ExecuteCode onSelectTable(MycatDataContext dataContext, SQLTableSource tableSource,
                                        SQLRequest<SQLSelectStatement> request, Response receiver) {
        SQLContext sqlContext = request.getContext();
        SQLSelectStatement statement = request.getAst();
        MycatDBContext mycatDBContext = sqlContext.getMycatDBContext();


        ASTCheckCollector collector = new ASTCheckCollector(statement);
        tableSource.accept(collector);
        collector.endVisit();

        if (collector.getErrors().size() > 0) {
            /*检测出存在不支持的错误语法*/
            receiver.sendError(collector.getErrors().get(0));
            return ExecuteCode.PROXY_ERROR;
        }

        if (collector.isDual()) {
            /*select 1 from dual; select 1; 空表查询*/
            return onSelectDual(request, receiver);
        }

        ///////////////////////////////booster//////////////////////////////
        if (!dataContext.isInTransaction() && dataContext.isAutocommit()) {
            Optional<String> booster = BoosterRuntime.INSTANCE.getBooster(dataContext.getUser().getUserName());
            if (booster.isPresent()) {
                receiver.proxySelect(booster.get(), statement);
                return ExecuteCode.PERFORMED;
            }
        }

        ///////////////////////////////common///////////////////////////////
        Map<String, SchemaHandler> schemaMap = mycatDBContext.config().getSchemaMap();
        String schemaName = Optional.ofNullable(collector.getSchema()).orElse(dataContext.getDefaultSchema());
        if (schemaName == null) {
            receiver.sendError(new MycatException("schema is null"));
            return ExecuteCode.PERFORMED;
        }
        Set<String> tables = collector.getTables();
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler == null) {
            String defaultSchema = sqlContext.getDefaultSchema();
            if (defaultSchema != null) {
                schemaHandler = schemaMap.get(defaultSchema);
            } else if (schemaName != null) {
                Optional<String> targetNameOptional = Optional.of(RootHelper.INSTANCE)
                        .map(RootHelper::getConfigProvider)
                        .map(ConfigProvider::currentConfig)
                        .map(MycatConfig::getMetadata)
                        .map(ShardingQueryRootConfig::getPrototype)
                        .map(ShardingQueryRootConfig.PrototypeServer::getTargetName);
                if (targetNameOptional.isPresent()) {
                    receiver.proxySelect(targetNameOptional.get(), statement);
                    return ExecuteCode.PERFORMED;
                } else {
                    receiver.proxySelect(ReplicaSelectorRuntime.INSTANCE.getPrototypeOrFirstReplicaDataSource(), statement);
                    return ExecuteCode.PERFORMED;
                }
            } else {
                receiver.proxySelect(ReplicaSelectorRuntime.INSTANCE.getPrototypeOrFirstReplicaDataSource(), statement);
                return ExecuteCode.PERFORMED;
            }
        }


        ///////////////////////////////common///////////////////////////////

        TableHandler tableHandlerEntry = chooseTableHandler(schemaHandler.logicTables(), tables);
        if (tableHandlerEntry == null) {
            receiver.proxySelect(schemaHandler.defaultTargetName(), statement);
            return ExecuteCode.PERFORMED;
        }

        if (false) {
            ParseContext parseContext = ParseContext.of(dataContext.getDefaultSchema(), statement);
            Schema plan = parseContext.getPlan();
            HBTQueryConvertor2 hbtQueryConvertor2 = new HBTQueryConvertor2();
            ResultHandler resultHandler = hbtQueryConvertor2.complie(plan);
            if (resultHandler instanceof InputHandler) {
                InputHandler resultHandler1 = (InputHandler) resultHandler;
                String targetName = resultHandler1.getTargetName();
                String sql = resultHandler1.getSql();
                receiver.proxySelect(targetName, sql);
                return ExecuteCode.PERFORMED;
            }
            HBTRunners hbtRunners = new HBTRunners(mycatDBContext);
            RowBaseIterator run = hbtRunners.run(plan);
            receiver.sendResultSet(() -> run, null);
            return ExecuteCode.PERFORMED;
        }
        dataContext.block(() -> {
            try {
                ///////////////////////////////////mycatdb////////////////////////////////////////////////
                MycatDBSharedServer uponDBSharedServer = mycatDBContext.getUponDBSharedServer();

                MycatSQLPrepareObject mycatSQLPrepareObject = uponDBSharedServer
                        .innerQueryPrepareObject(statement.toString(), mycatDBContext);
                PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
                if (plan instanceof MycatSqlPlanner) {
                    ProxyInfo proxyInfo = ((MycatSqlPlanner) plan).tryGetProxyInfo();
                    if (proxyInfo != null) {
                        String sql = proxyInfo.getSql();
                        String targetName = proxyInfo.getTargetName();
                        receiver.proxySelect(targetName, sql);
                        return;
                    }
                }
                receiver.sendResultSet(() -> plan.run(), plan::explain);
            } catch (Throwable e) {
                receiver.sendError(e);
            }
        });

        return ExecuteCode.PERFORMED;
    }

    private TableHandler chooseTableHandler(Map<String, TableHandler> tableMap, Set<String> tables) {
        for (String table : tables) {
            TableHandler tableHandler = tableMap.get(table);
            if (tableHandler != null) {
                return tableHandler;
            }
        }
        return null;
    }

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) {
        //直接调用已实现好的
        SQLSelectStatement ast = request.getAst();
        TableSourceExtractor tableSourceExtractor = new TableSourceExtractor();
        if (hanldeInformationSchema(response, ast, tableSourceExtractor)) {
            return ExecuteCode.PERFORMED;
        }
        if (tableSourceExtractor.getTableSources().isEmpty()) {
            return onSelectNoTable(request, response);
        }
        return onSelectTable(dataContext, tableSourceExtractor.getTableSources().get(0), request, response);
    }

    private boolean hanldeInformationSchema(Response response, SQLSelectStatement ast, TableSourceExtractor tableSourceExtractor) {

        ast.accept(tableSourceExtractor);
        boolean cantainsInformation_schema = tableSourceExtractor.getTableSources().stream().anyMatch(new Predicate<SQLExprTableSource>() {
            @Override
            public boolean test(SQLExprTableSource sqlExprTableSource) {
                SQLExpr expr = sqlExprTableSource.getExpr();
                if (expr instanceof SQLPropertyExpr) {
                    SQLExpr owner = ((SQLPropertyExpr) expr).getOwner();
                    if (owner instanceof SQLIdentifierExpr) {
                        return "information_schema".equalsIgnoreCase(((SQLIdentifierExpr) owner).normalizedName());
                    }
                    return "information_schema".equalsIgnoreCase(((SQLPropertyExpr) expr).getName());

                }
                return false;
            }
        });
        if (cantainsInformation_schema) {
            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(ReplicaSelectorRuntime.INSTANCE.getPrototypeOrFirstReplicaDataSource())) {
                try (RowBaseIterator rowBaseIterator = connection.executeQuery(ast.toString())) {
                    response.sendResultSet(() -> rowBaseIterator, () -> Arrays.asList(ast.toString()));
                    return true;
                }
            }
        }
        return false;
    }

    @Getter
    private static class ASTCheckCollector extends MySqlASTVisitorAdapter {
        /*select * from db1.table1,db2.table2*/
        private final Set<SQLExprTableSource> tableSources = new LinkedHashSet<>();
        private final Set<String> tables = new LinkedHashSet<>();
        private final SQLSelectStatement statement;
        private String schema;
        private List<MycatException> errors = new ArrayList<>();
        private boolean dual = false;

        public ASTCheckCollector(SQLSelectStatement statement) {
            this.statement = statement;
        }

        public void endVisit() {
//            if(this.schema == null || this.schema.isEmpty()){
//                this.errors.add(new MycatException("unknown schema. sql={};\n", statement));
//            }
            if (this.dual && tables.size() > 1) {
                this.errors.add(new MycatException("only support one simple dual. no support multiple table. sql={};\n", statement));
            }
        }

        @Override
        public boolean visit(SQLExprTableSource tableSource) {
            String visitSchema = normalizeAndLowerCase(tableSource.getSchema());
            if (visitSchema != null) {
                if (this.schema == null) {
                    this.schema = visitSchema;
                } else if (!Objects.equals(this.schema, visitSchema)) {
                }
            }

            String table = normalizeAndLowerCase(tableSource.getTableName());
            if (!this.dual && "dual".equals(table)) {
                this.dual = true;
            }
            this.tables.add(table);
            this.tableSources.add(tableSource);
            return this.errors.isEmpty();
        }

        private static String normalizeAndLowerCase(String str) {
            if (str == null) {
                return null;
            }
            return normalize(str);
        }
    }

    @Override
    protected ExecuteCode onExplain(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) {
        response.setExplainMode(true);
        return onExecute(request, dataContext, response);
    }

    public static String normalize(String sql) {
        if (sql == null) {
            return null;
        }
        if ("''".equals(sql)) {
            return "";
        }
        return SQLUtils.normalize(sql, DbType.mysql);
    }
}
