package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLShowCreateTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;


public class ShowCreateTableSQLHandler extends AbstractSQLHandler<SQLShowCreateTableStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLShowCreateTableStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLShowCreateTableStatement ast = request.getAst();
        SQLName name = ast.getName();
        if (name instanceof SQLIdentifierExpr){
            SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
            sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
            sqlPropertyExpr.setName(name.toString());
            ast.setName(sqlPropertyExpr);
        }
        response.tryBroadcastShow(ast.toString());
        return ;
//
//        SQLName nameExpr = ast.getName();
//        if (nameExpr == null) {
//            response.sendError(new MycatException("table name is null"));
//            return ExecuteCode.PERFORMED;
//        }
//        String schemaName = dataContext.getDefaultSchema();
//        String tableName;
//        if (nameExpr instanceof SQLIdentifierExpr) {
//            tableName = ((SQLIdentifierExpr) nameExpr).normalizedName();
//        }else if (nameExpr instanceof SQLPropertyExpr){
//            schemaName =
//                    ((SQLIdentifierExpr)((SQLPropertyExpr) nameExpr).getOwner()).normalizedName();
//            tableName = SQLUtils.normalize(((SQLPropertyExpr) nameExpr).getName());
//        }else {
//            response.proxyShow(ast);
//            return ExecuteCode.PERFORMED;
//        }
//        ast.setName(new SQLPropertyExpr(schemaName,tableName));
//
//        TableHandler table = MetadataManager.INSTANCE.getTable(schemaName, tableName);
//        if (table == null){
//            String finalSchemaName = schemaName;
//            String s = Optional.ofNullable(MetadataManager.INSTANCE.getSchemaMap()).map(i -> i.get(finalSchemaName)
//            ).map(i -> i.defaultTargetName()).orElse(null);
//            if (s==null){
//                response.proxyShow(ast);
//            }else {
//                response.proxySelect(s,ast.toString());
//            }
//            return ExecuteCode.PERFORMED;
//        }
//        String createTableSQL = table.getCreateTableSQL();
//
//        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
//        resultSetBuilder.addColumnInfo("Table", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("Create Table", JDBCType.VARCHAR);
//        resultSetBuilder.addObjectRowPayload(Arrays.asList(table.getTableName(),createTableSQL));
//        response.sendResultSet(()->resultSetBuilder.build(),()->{throw  new UnsupportedOperationException();});
//        return ExecuteCode.PERFORMED;
    }
}
