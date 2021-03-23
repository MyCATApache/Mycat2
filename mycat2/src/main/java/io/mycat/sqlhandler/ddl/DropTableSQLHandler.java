package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.config.MycatRouterConfigOps;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ConfigUpdater;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;


public class DropTableSQLHandler extends AbstractSQLHandler<SQLDropTableStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DropTableSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropTableStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLockWithTimeout(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
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
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });

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
