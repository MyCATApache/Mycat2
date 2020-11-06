package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateDatabaseStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatRouterConfigOps;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.SqlHints;
import io.mycat.util.JsonUtil;
import io.mycat.util.Response;

import java.util.Map;
import java.util.Optional;


public class CreateDatabaseSQLHandler extends AbstractSQLHandler<SQLCreateDatabaseStatement> {


    @Override
    protected void onExecute(SQLRequest<SQLCreateDatabaseStatement> request, MycatDataContext dataContext, Response response) {
        SQLCreateDatabaseStatement ast = request.getAst();
        boolean ifNotExists = ast.isIfNotExists();
        String name = SQLUtils.normalize(ast.getName().getSimpleName());
        Map<String, Object> attributes = ast.getAttributes();
        String json = (String) attributes.get(SqlHints.AFTER_COMMENT);
        String targetName = JsonUtil.fromMap(json, "targetName").orElse(null);
        try(MycatRouterConfigOps ops = ConfigUpdater.getOps()){
            ops.addSchema(name,targetName);
            ops.commit();
            response.sendOk();
        }
    }
}
