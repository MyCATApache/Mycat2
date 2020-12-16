package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLDropSequenceStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;


public class DropSequenceSQLHandler extends AbstractSQLHandler<com.alibaba.fastsql.sql.ast.statement.SQLDropSequenceStatement> {

    @Override
    protected void onExecute(SQLRequest<com.alibaba.fastsql.sql.ast.statement.SQLDropSequenceStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLDropSequenceStatement ast = request.getAst();
        SQLName name = ast.getName();
        if (name instanceof SQLIdentifierExpr) {
            SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
            sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
            sqlPropertyExpr.setName(name.toString());
            ast.setName(sqlPropertyExpr);
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        response.proxyUpdate(metadataManager.getPrototype(), ast.toString());
    }
}
