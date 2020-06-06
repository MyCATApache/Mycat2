package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLShowCreateTableStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.TableHandler;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.sql.JDBCType;
import java.util.Arrays;

@Resource
public class ShowCreateTableSQLHandler extends AbstractSQLHandler<SQLShowCreateTableStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowCreateTableStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowCreateTableStatement ast = request.getAst();

        SQLName nameExpr = ast.getName();
        if (nameExpr == null) {
            response.sendError(new MycatException("table name is null"));
            return ExecuteCode.PERFORMED;
        }
        String schemaName = dataContext.getDefaultSchema();
        String tableName;
        if (nameExpr instanceof SQLIdentifierExpr) {
            tableName = ((SQLIdentifierExpr) nameExpr).normalizedName();
        }else if (nameExpr instanceof SQLPropertyExpr){
            schemaName =
                    ((SQLIdentifierExpr)((SQLPropertyExpr) nameExpr).getOwner()).normalizedName();
            tableName = SQLUtils.normalize(((SQLPropertyExpr) nameExpr).getName());
        }else {
            response.sendError(new MycatException("unsupport name :"+nameExpr));
            return ExecuteCode.PERFORMED;
        }
        TableHandler table = MetadataManager.INSTANCE.getTable(schemaName, tableName);
        if (table == null){
            response.sendError(new MycatException("table "+ schemaName+"."+tableName+" is not existed"));
            return ExecuteCode.PERFORMED;
        }
        String createTableSQL = table.getCreateTableSQL();

        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("Table", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Create Table", JDBCType.VARCHAR);
        resultSetBuilder.addObjectRowPayload(Arrays.asList(table.getTableName(),createTableSQL));
        response.sendResultSet(()->resultSetBuilder.build(),()->{throw  new UnsupportedOperationException();});
        return ExecuteCode.PERFORMED;
    }
}
