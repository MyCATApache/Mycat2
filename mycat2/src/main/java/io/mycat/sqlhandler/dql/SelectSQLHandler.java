package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.MycatConfig;
import io.mycat.MycatException;
import io.mycat.RootHelper;
import io.mycat.calcite.prepare.MycatSQLPrepareObject;
import io.mycat.calcite.prepare.MycatSqlPlanner;
import io.mycat.metadata.SchemaHandler;
import io.mycat.metadata.TableHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.meta.MetadataService;
import io.mycat.proxy.session.MycatSession;
import io.mycat.upondb.MycatDBContext;
import io.mycat.upondb.MycatDBSharedServer;
import io.mycat.upondb.PlanRunner;
import io.mycat.upondb.ProxyInfo;
import io.mycat.util.Receiver;
import io.mycat.util.SQLContext;
import lombok.Data;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@Resource
public class SelectSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Resource
    private MetadataService mycatMetadataService;

    @PostConstruct
    public void init(){

    }

    @Override
    protected int onExecute(SQLRequest<SQLSelectStatement> request, Receiver response, MycatSession session) {
        //直接调用已实现好的
        SQLSelectStatement ast = request.getAst();

        Optional<SQLSelectQueryBlock> sqlSelectQueryBlockMaybe = Optional.ofNullable(ast)
                .map(SQLSelectStatement::getSelect)
                .map(SQLSelect::getQueryBlock);
        Optional<SQLTableSource> sqlTableSource = sqlSelectQueryBlockMaybe.map(SQLSelectQueryBlock::getFrom);
        int returnCode = sqlTableSource
                .map(tableSource -> onSelectTable(tableSource, sqlSelectQueryBlockMaybe.get(), request, response))
                .orElseGet(() -> onSelectNoTable(sqlSelectQueryBlockMaybe.orElse(null), request, response));
        return returnCode;
    }

    protected int onSelectNoTable(SQLSelectQueryBlock sqlSelectQueryBlock,
                      SQLRequest<SQLSelectStatement> request, Receiver receiver){
        SQLContext sqlContext = request.getContext();
        SQLSelectStatement statement = request.getAst();
        receiver.evalSimpleSql(statement);
        return CODE_200;
    }

    protected int onSelectTable(SQLTableSource tableSource,SQLSelectQueryBlock sqlSelectQueryBlock,
                      SQLRequest<SQLSelectStatement> request, Receiver receiver){
        SQLContext sqlContext = request.getContext();
        SQLSelectStatement statement = request.getAst();
        MycatDBContext mycatDBContext = sqlContext.getMycatDBContext();

        //  有表sql
        boolean forUpdate = sqlSelectQueryBlock.isForUpdate();

        TableCollector tableCollector = new TableCollector();
        tableSource.accept(tableCollector);
        SQLExprTableSource someTables = tableCollector.getSomeTables();
        if(someTables.getSchema() == null) {
            receiver.sendError(new MycatException("unknown schema. sql={}",statement));
            return CODE_300;
        }
        if(someTables.getTableName() == null) {
            receiver.sendError(new MycatException("unknown tableName. sql={}",statement));
            return CODE_300;
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
            } else if(schemaName != null){
                MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();
                String targetName = mycatConfig.getMetadata().getPrototype().getTargetName();
                if(targetName != null){
                    receiver.proxySelect(targetName,statement);
                    return CODE_200;
                }else {
                    receiver.sendError(new MycatException("unknown schema={}",schemaName));
                    return CODE_300;
                }
            } else {
                receiver.sendError(new MycatException("unknown schema"));
                return CODE_300;
            }
        }
        String defaultTargetName = schemaHandler.defaultTargetName();
        Map<String, TableHandler> tableMap = schemaHandler.logicTables();
        TableHandler tableHandler = tableMap.get(tableName);
        ///////////////////////////////common///////////////////////////////

        if (tableHandler == null) {
            receiver.proxySelect(defaultTargetName, statement);
            return CODE_200;
        }
        MycatDBSharedServer uponDBSharedServer = mycatDBContext.getUponDBSharedServer();
        MycatSQLPrepareObject mycatSQLPrepareObject = uponDBSharedServer
                .innerQueryPrepareObject(statement.toString(), mycatDBContext);
        PlanRunner plan = mycatSQLPrepareObject.plan(Collections.emptyList());
        if (plan instanceof MycatSqlPlanner) {
            ProxyInfo proxyInfo = ((MycatSqlPlanner) plan).tryGetProxyInfo();
            if (proxyInfo!=null) {
                String sql = proxyInfo.getSql();
                String targetName = proxyInfo.getTargetName();
                boolean updateOpt = proxyInfo.isUpdateOpt();
                receiver.proxySelect(targetName, sql);
                return CODE_200;
            }
        }
        receiver.eval(plan);
        return CODE_200;
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
}
