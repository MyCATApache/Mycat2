package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.SqlHints;
import io.mycat.util.JsonUtil;
import io.mycat.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class CreateDatabaseSQLHandler extends AbstractSQLHandler<SQLCreateDatabaseStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDatabaseSQLHandler.class);


    @Override
    protected void onExecute(SQLRequest<SQLCreateDatabaseStatement> request, MycatDataContext dataContext, Response response)  throws Exception {
        SQLCreateDatabaseStatement ast = request.getAst();
        boolean ifNotExists = ast.isIfNotExists();
        String tableName = SQLUtils.normalize(ast.getName().getSimpleName());
        Map<String, Object> attributes = ast.getAttributes();
        String json = (String) attributes.get(SqlHints.AFTER_COMMENT);
        String targetName = JsonUtil.fromMap(json, "targetName").orElse(null);
        try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
            ops.addSchema(tableName, targetName);
            ops.commit();
            onPhysics(tableName);
            response.sendOk();
        }

    }

    protected void onPhysics(String name) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
            connection.executeUpdate(String.format(
                    "CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;",
                    name),false);
        }catch (Throwable t){
            LOGGER.warn("",t);
        }
    }
}
