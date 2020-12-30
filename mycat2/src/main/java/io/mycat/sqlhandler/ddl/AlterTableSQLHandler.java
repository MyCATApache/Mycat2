package io.mycat.sqlhandler.ddl;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.calcite.table.GlobalTableHandler;
import io.mycat.calcite.table.NormalTableHandler;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.router.ShardingTableHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;

import java.util.Collections;
import java.util.List;


public class AlterTableSQLHandler extends AbstractSQLHandler<SQLAlterTableStatement> {

    @Override
    protected void onExecute(SQLRequest<SQLAlterTableStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLAlterTableStatement sqlAlterTableStatement = request.getAst();
        if (sqlAlterTableStatement.getSchema() == null) {
            SQLExprTableSource tableSource = sqlAlterTableStatement.getTableSource();
            String defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema == null) {
                throw new MycatException("please use schema");
            }
            tableSource.setSchema(defaultSchema);
        }
        String schema = SQLUtils.normalize(sqlAlterTableStatement.getSchema());
        String tableName = SQLUtils.normalize(sqlAlterTableStatement.getTableName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schema, tableName);
        MySqlCreateTableStatement createTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(tableHandler.getCreateTableSQL());
        boolean changed = createTableStatement.apply(sqlAlterTableStatement);
        if (changed) {
            JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            SQLExprTableSource tableSource = createTableStatement.getTableSource();
            List<DataNode> dataNodes;
            switch (tableHandler.getType()) {
                case SHARDING: {
                    ShardingTableHandler handler = (ShardingTableHandler) tableHandler;
                    dataNodes = handler.dataNodes();
                    break;
                }
                case GLOBAL: {
                    GlobalTableHandler handler = (GlobalTableHandler) tableHandler;
                    dataNodes = handler.getGlobalDataNode();
                    break;
                }
                case NORMAL: {
                    NormalTableHandler handler = (NormalTableHandler) tableHandler;
                    dataNodes = Collections.singletonList(handler.getDataNode());
                    break;
                }
                case CUSTOM:
                default:
                    throw MycatErrorCode.createMycatException(MycatErrorCode.ERR_NOT_SUPPORT,"alter custom table supported");
            }
            executeAlter(sqlAlterTableStatement, connectionManager, tableSource, dataNodes);
            CreateTableSQLHandler.INSTANCE.createTable(Collections.emptyMap(),schema,tableName,createTableStatement);
        }
        response.sendOk();
    }

    private void executeAlter(SQLAlterTableStatement alterTableStatement, JdbcConnectionManager connectionManager, SQLExprTableSource tableSource, List<DataNode> dataNodes) {
        for (DataNode dataNode : dataNodes) {
            tableSource.setSimpleName(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            String sql = alterTableStatement.toString();
            try (DefaultConnection connection = connectionManager.getConnection(dataNode.getTargetName())) {
                connection.executeUpdate(sql, false);
            }
        }
    }
}
