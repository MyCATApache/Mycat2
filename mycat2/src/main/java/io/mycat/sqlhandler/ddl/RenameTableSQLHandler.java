package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class RenameTableSQLHandler extends AbstractSQLHandler<MySqlRenameTableStatement> {

    @Override
    protected PromiseInternal<Void> onExecute(SQLRequest<MySqlRenameTableStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MySqlRenameTableStatement mySqlRenameTableStatement = request.getAst();
        for (MySqlRenameTableStatement.Item item : mySqlRenameTableStatement.getItems()) {
            SQLName name = item.getName();
            if (name instanceof SQLIdentifierExpr) {
                checkDefaultSchemaNotNull(dataContext);
                item.setName(new SQLPropertyExpr(dataContext.getDefaultSchema(), name.getSimpleName()));
            }
            SQLName to = item.getTo();
            if (to instanceof SQLIdentifierExpr) {
                checkDefaultSchemaNotNull(dataContext);
                item.setTo(new SQLPropertyExpr(dataContext.getDefaultSchema(), to.getSimpleName()));
            }
        }
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        for (MySqlRenameTableStatement.Item item : new ArrayList<>(mySqlRenameTableStatement.getItems())) {
            MySqlRenameTableStatement sqlRenameTableStatement = clone(mySqlRenameTableStatement);
            sqlRenameTableStatement.getItems().clear();
            sqlRenameTableStatement.addItem(item);

            SQLPropertyExpr name = (SQLPropertyExpr) item.getName();
            TableHandler tableHandler = metadataManager.getTable(name.getOwnernName(), name.getName());
            executeOnPrototype(sqlRenameTableStatement, jdbcConnectionManager);
            executeOnDataNodes(sqlRenameTableStatement, jdbcConnectionManager, tableHandler);

            String createTableSQL = tableHandler.getCreateTableSQL();
            MySqlCreateTableStatement sqlStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSQL);
            CreateTableSQLHandler.INSTANCE.createTable(Collections.emptyMap(),
                    tableHandler.getSchemaName(),
                    tableHandler.getTableName(), sqlStatement);
        }


        return response.sendOk();
    }

    private MySqlRenameTableStatement clone(MySqlRenameTableStatement mySqlRenameTableStatement) {
        return (MySqlRenameTableStatement)
                SQLUtils.parseSingleMysqlStatement(mySqlRenameTableStatement.toString());
    }

    public void executeOnDataNodes(MySqlRenameTableStatement sqlStatement,
                                   JdbcConnectionManager connectionManager,
                                   TableHandler tableHandler) {
        List<DataNode> dataNodes = getDataNodes(tableHandler);
        switch (tableHandler.getType()) {
            case SHARDING:
                executeOnPrototype(sqlStatement, connectionManager);
                break;
            case GLOBAL:
            case NORMAL:
                executeOnPrototype(sqlStatement, connectionManager);
                for (DataNode dataNode : dataNodes) {
                    MySqlRenameTableStatement each = clone(sqlStatement);
                    String sql = each.toString();
                    try (DefaultConnection connection = connectionManager.getConnection(dataNode.getTargetName())) {
                        connection.executeUpdate(sql, false);
                    }
                }
                break;
            case CUSTOM:
            default:
                break;
        }
    }

}
