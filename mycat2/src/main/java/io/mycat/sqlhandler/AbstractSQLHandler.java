package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.calcite.table.GlobalTableHandler;
import io.mycat.calcite.table.NormalTableHandler;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.ClassUtil;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@EqualsAndHashCode
public abstract class AbstractSQLHandler<Statement extends SQLStatement> implements SQLHandler<Statement> {
    private final Class statementClass;

    public AbstractSQLHandler() {
        Class<?> statement = ClassUtil.findGenericType(this, AbstractSQLHandler.class, "Statement");
        Objects.requireNonNull(statement);
        statementClass = statement;
    }

    public AbstractSQLHandler(Class statementClass) {
        this.statementClass = statementClass;
    }

    @Override
    public void execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) throws Exception {
        try {
            onExecuteBefore(request, dataContext, response);
            onExecute(request, dataContext, response);
        } finally {
            onExecuteAfter(request, dataContext, response);
        }
    }

    protected void onExecuteBefore(SQLRequest<Statement> request, MycatDataContext dataContext, Response respons) {
    }

    protected abstract void onExecute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) throws Exception;

    protected void onExecuteAfter(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) throws Exception {


    }

    public Class getStatementClass() {
        return statementClass;
    }

    public void resolveSQLExprTableSource( SQLExprTableSource tableSource,MycatDataContext dataContext) {
        if (tableSource.getSchema() == null) {
            String defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema == null) {
                throw new MycatException("please use schema");
            }
            tableSource.setSchema(defaultSchema);
        }
    }

    public void executeOnPrototype(SQLStatement sqlStatement,
                                   JdbcConnectionManager connectionManager) {
        try(DefaultConnection connection = connectionManager.getConnection("prototype")){
            connection.executeUpdate(sqlStatement.toString(),false);
        }
    }
    public void executeOnDataNodes(SQLStatement sqlStatement, JdbcConnectionManager connectionManager, List<DataNode> dataNodes, SQLExprTableSource tableSource) {
        for (DataNode dataNode : dataNodes) {
            tableSource.setSimpleName(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            String sql = sqlStatement.toString();
            try (DefaultConnection connection = connectionManager.getConnection(dataNode.getTargetName())) {
                connection.executeUpdate(sql, false);
            }
        }
    }

    public List<DataNode> getDataNodes(TableHandler tableHandler) {
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
        return dataNodes;
    }
}
