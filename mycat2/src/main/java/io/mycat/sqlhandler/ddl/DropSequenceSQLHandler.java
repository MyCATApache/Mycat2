package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.impl.future.PromiseInternal;


public class DropSequenceSQLHandler extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLDropSequenceStatement ast = request.getAst();
        SQLName name = ast.getName();
        if (name instanceof SQLIdentifierExpr) {
            SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
            sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
            sqlPropertyExpr.setName(name.toString());
            ast.setName(sqlPropertyExpr);
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        return response.proxyUpdate(metadataManager.getPrototype(), ast.toString());
    }
}
