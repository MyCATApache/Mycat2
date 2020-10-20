package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLDropDatabaseStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.metadata.DDLOps;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;




public class DropDatabaseSQLHandler extends AbstractSQLHandler<SQLDropDatabaseStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLDropDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        SQLDropDatabaseStatement dropDatabaseStatement = request.getAst();
        String schemaName = SQLUtils.normalize(dropDatabaseStatement.getDatabaseName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        try(DDLOps ddlObject = metadataManager.startDDL()){
            ddlObject.dropSchema(schemaName);
            ddlObject.commit();
            response.sendOk();
        }
    }
}
