package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.MetadataManager;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;


public class ShowTablesSQLHandler extends AbstractSQLHandler<SQLShowTablesStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowTablesSQLHandler.class);

    @Override
    protected void onExecute(SQLRequest<SQLShowTablesStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLShowTablesStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        SQLName database = ast.getDatabase();
        if (database == null){
            response.sendError(new MycatException("NO DATABASES SELECTED"));
            return ;
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        Optional<SchemaHandler> schemaHandler = Optional.ofNullable(metadataManager.getSchemaMap()).map(i -> i.get(SQLUtils.normalize(ast.getDatabase().toString())));
        String targetName = schemaHandler.map(i -> i.defaultTargetName()).map(name ->dataContext.resolveDatasourceTargetName(name)).orElse(null);
        if (targetName != null) {
            response.proxySelect(targetName, ast.toString());
        } else {
            response.proxySelectToPrototype(ast.toString());
        }
//        DDLManager.INSTANCE.updateTables();
//        String sql = ShowStatementRewriter.rewriteShowTables(dataContext.getDefaultSchema(), request.getAst());
//        LOGGER.info(sql);
//        //show 语句变成select 语句
//
//        try (RowBaseIterator query = MycatDBs.createClient(dataContext).query(sql)) {
//            //schema上默认的targetName;
//            try {
//                SQLShowTablesStatement showTablesStatement = request.getAst();
//                SQLName from = showTablesStatement.getFrom();
//                String schema = SQLUtils.normalize(from == null ? dataContext.getDefaultSchema() : from.getSimpleName());
//                if (WithDefaultTargetInfo(response, sql, query, schema)) return ExecuteCode.PERFORMED;
//            } catch (Exception e) {
//                LOGGER.error("", e);
//            }
//            response.sendResultSet(()->query, null);
//            return ExecuteCode.PERFORMED;
//        }
//        response.proxyShow(ast);
        return ;
    }

//    private boolean WithDefaultTargetInfo(Response response, String sql, RowBaseIterator query, String schema) {
//        if (schema != null) {
//            String defaultTargetName = Optional.ofNullable(MetadataManager.INSTANCE)
//                    .map(i -> i.getSchemaMap())
//                    .map(i -> i.get(schema))
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                    //暂时定为没有配置分片表才读取默认targetName的表作为tables
//                    .filter(i -> i.logicTables() == null || i.logicTables().isEmpty())
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//                    .map(i -> i.defaultTargetName())
//                    .orElse(null);
//            if (defaultTargetName != null) {
//                defaultTargetName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(defaultTargetName, true, null);
//                try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(defaultTargetName)) {
//                    RowBaseIterator rowBaseIterator = connection.executeQuery(sql);
//
//                    //safe
//                    response.sendResultSet(() -> ComposeRowBaseIterator.of(rowBaseIterator, query));
//                    return true;
//                }
//            }
//        }
//        return false;
//    }


}
