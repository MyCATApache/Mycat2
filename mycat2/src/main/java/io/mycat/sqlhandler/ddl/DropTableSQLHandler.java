package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class DropTableSQLHandler extends AbstractSQLHandler<SQLDropTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropTableSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropTableStatement> request, MycatDataContext dataContext, Response response) {
        try {
            SQLDropTableStatement ast = request.getAst();
            List<SQLExprTableSource> tableSources = ast.getTableSources();
            if (tableSources.size() != 1) {
                throw new UnsupportedOperationException("unsupported drop multi table :" + tableSources.get(0));
            }
            SQLExprTableSource tableSource = ast.getTableSources().get(0);
            String schema = SQLUtils.normalize(
                    tableSource.getSchema() == null ?
                            dataContext.getDefaultSchema() : tableSource.getSchema()
            );
            String tableName = SQLUtils.normalize(
                    tableSource.getTableName()
            );
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.removeTable(schema, tableName);
                ops.commit();
                onPhysics(schema, tableName);
                return response.sendOk();
            }
        }catch (Throwable throwable){
            return response.sendError(throwable);
        }
    }

    protected void onPhysics(String schema, String tableName) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection(metadataManager.getPrototype())) {
            connection.executeUpdate(String.format(
                    "DROP TABLE IF EXISTS %s;",
                    schema+"."+ tableName),false);
        }catch (Throwable t){
            LOGGER.warn("",t);
        }
    }
}
