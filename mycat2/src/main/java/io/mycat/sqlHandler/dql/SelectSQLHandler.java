package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.calcite.prepare.MycatSQLPrepareObject;
import io.mycat.calcite.prepare.MycatSqlPlanner;
import io.mycat.metadata.SchemaHandler;
import io.mycat.metadata.TableHandler;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.MycatDBSharedServer;
import io.mycat.upondb.ProxyInfo;
import io.mycat.util.Response;
import io.mycat.util.SQLContext;
import lombok.Data;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@Resource
public class SelectSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    public SelectSQLHandler() {
    }

    public SelectSQLHandler(Class statementClass) {
        super(statementClass);
    }


    protected ExecuteCode onSelectNoTable(SQLSelectQueryBlock sqlSelectQueryBlock,
                                          SQLRequest<SQLSelectStatement> request, Response receiver) {
        SQLContext sqlContext = request.getContext();
        SQLSelectStatement statement = request.getAst();
        receiver.evalSimpleSql(statement);
        return ExecuteCode.PERFORMED;
    }

    protected ExecuteCode onSelectTable(SQLTableSource tableSource, SQLSelectQueryBlock sqlSelectQueryBlock,
                                        SQLRequest<SQLSelectStatement> request, Response receiver) {
        SQLContext sqlContext = request.getContext();
        SQLSelectStatement statement = request.getAst();
        MycatDBContext mycatDBContext = sqlContext.getMycatDBContext();

        //  有表sql
        boolean forUpdate = sqlSelectQueryBlock.isForUpdate();

        TableCollector tableCollector = new TableCollector();
        tableSource.accept(tableCollector);
        SQLExprTableSource someTables = tableCollector.getSomeTables();
        if (someTables.getSchema() == null) {
            receiver.sendError(new MycatException("unknown schema. sql={}", statement));
            return ExecuteCode.PROXY_ERROR;
        }
        if (someTables.getTableName() == null) {
            receiver.sendError(new MycatException("unknown tableName. sql={}", statement));
            return ExecuteCode.PROXY_ERROR;
        }
        String schemaName = SQLUtils.normalize(someTables.getSchema().toLowerCase());
        String tableName = SQLUtils.normalize(someTables.getTableName().toLowerCase());


        ///////////////////////////////common///////////////////////////////
        Map<String, SchemaHandler> schemaMap = mycatDBContext.config().getSchemaMap();
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler == null) {
            String defaultSchema = sqlContext.getDefaultSchema();
            if (defaultSchema != null) {
                schemaHandler = schemaMap.get(defaultSchema);
            } else if (schemaName != null) {
                Optional<String> targetNameOptional = Optional.ofNullable(RootHelper.INSTANCE)
                        .map(i -> i.getConfigProvider())
                        .map(i -> i.currentConfig())
                        .map(i->i.getMetadata())
                        .map(i->i.getPrototype())
                        .map(i->i.getTargetName());
                if (targetNameOptional.isPresent()) {
                    receiver.proxySelect(targetNameOptional.get(), statement);
                    return ExecuteCode.PERFORMED;
                } else {
                    receiver.proxySelect(ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource(),statement);
                    return ExecuteCode.PERFORMED;
                }
            } else {
                receiver.proxySelect(ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource(),statement);
                return ExecuteCode.PERFORMED;
            }
        }
        String defaultTargetName = schemaHandler.defaultTargetName();
        Map<String, TableHandler> tableMap = schemaHandler.logicTables();
        TableHandler tableHandler = tableMap.get(tableName);
        ///////////////////////////////common///////////////////////////////

        if (tableHandler == null) {
            receiver.proxySelect(defaultTargetName, statement);
            return ExecuteCode.PERFORMED;
        }
        MycatDBSharedServer uponDBSharedServer = mycatDBContext.getUponDBSharedServer();
        MycatSQLPrepareObject mycatSQLPrepareObject = uponDBSharedServer
                .innerQueryPrepareObject(statement.toString(), mycatDBContext);
        PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
        if (plan instanceof MycatSqlPlanner) {
            ProxyInfo proxyInfo = ((MycatSqlPlanner) plan).tryGetProxyInfo();
            if (proxyInfo != null) {
                String sql = proxyInfo.getSql();
                String targetName = proxyInfo.getTargetName();
                boolean updateOpt = proxyInfo.isUpdateOpt();
                receiver.proxySelect(targetName, sql);
                return ExecuteCode.PERFORMED;
            }
        }
        receiver.sendResultSet(plan.run(), () -> plan.explain());
        return ExecuteCode.PERFORMED;
    }

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) {
        //直接调用已实现好的
        SQLSelectStatement ast = request.getAst();
        Optional<SQLSelectQueryBlock> sqlSelectQueryBlockMaybe = Optional.ofNullable(ast)
                .map(SQLSelectStatement::getSelect)
                .map(SQLSelect::getQueryBlock);
        Optional<SQLTableSource> sqlTableSource = sqlSelectQueryBlockMaybe.map(SQLSelectQueryBlock::getFrom);
        ExecuteCode returnCode = sqlTableSource
                .map(tableSource -> onSelectTable(tableSource, sqlSelectQueryBlockMaybe.get(), request, response))
                .orElseGet(() -> onSelectNoTable(sqlSelectQueryBlockMaybe.orElse(null), request, response));
        return returnCode;
    }

    @Data
    private static class TableCollector extends MySqlASTVisitorAdapter {
        private SQLExprTableSource someTables;

        @Override
        public boolean visit(SQLExprTableSource x) {
            someTables = x;
            return super.visit(x);
        }
    }
}
