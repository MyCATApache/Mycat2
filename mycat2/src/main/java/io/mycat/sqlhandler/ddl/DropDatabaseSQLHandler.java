package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLDropDatabaseStatement;
import io.mycat.*;
import io.mycat.config.MycatRouterConfig;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;


public class DropDatabaseSQLHandler extends AbstractSQLHandler<SQLDropDatabaseStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLDropDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        SQLDropDatabaseStatement dropDatabaseStatement = request.getAst();
        String schemaName = SQLUtils.normalize(dropDatabaseStatement.getDatabaseName());
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.dropSchema(schemaName);
            ops.commit();
            response.sendOk();
        }
    }
}
