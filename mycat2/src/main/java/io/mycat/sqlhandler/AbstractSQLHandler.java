/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import io.mycat.*;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.calcite.table.GlobalTableHandler;
import io.mycat.calcite.table.NormalTableHandler;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.ClassUtil;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import lombok.EqualsAndHashCode;
import org.jetbrains.annotations.NotNull;

import java.util.*;

@EqualsAndHashCode
public abstract class AbstractSQLHandler<Statement extends SQLStatement> implements SQLHandler<Statement> {
    private final Class statementClass;
    public final static String DDL_LOCK = "DDL_LOCK";
    public AbstractSQLHandler() {
        Class<?> statement = ClassUtil.findGenericType(this, AbstractSQLHandler.class, "Statement");
        Objects.requireNonNull(statement);
        statementClass = statement;
    }

    public AbstractSQLHandler(Class statementClass) {
        this.statementClass = statementClass;
    }

    @Override
    public Future<Void> execute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {
        try {
            onExecuteBefore(request, dataContext, response);
            return onExecute(request, dataContext, response);
        } finally {
            onExecuteAfter(request, dataContext, response);
        }
    }

    protected void onExecuteBefore(SQLRequest<Statement> request, MycatDataContext dataContext, Response respons) {
    }

    protected abstract Future<Void> onExecute(SQLRequest<Statement> request, MycatDataContext dataContext, Response response);

    protected void onExecuteAfter(SQLRequest<Statement> request, MycatDataContext dataContext, Response response) {


    }

    public Class getStatementClass() {
        return statementClass;
    }

    public void resolveSQLExprTableSource( SQLExprTableSource tableSource,MycatDataContext dataContext) {
        if (tableSource.getSchema() == null) {
            String defaultSchema = checkDefaultSchemaNotNull(dataContext);
            tableSource.setSchema(defaultSchema);
        }
    }

    @NotNull
    public String checkDefaultSchemaNotNull(MycatDataContext dataContext) {
        String defaultSchema = dataContext.getDefaultSchema();
        if (defaultSchema == null) {
            throw new MycatException("please use schema");
        }
        return defaultSchema;
    }

//    public void executeOnPrototype(SQLStatement sqlStatement,
//                                   JdbcConnectionManager connectionManager) {
//        try(DefaultConnection connection = connectionManager.getConnection("prototype")){
//            connection.executeUpdate(sqlStatement.toString(),false);
//        }
//    }
    public void executeOnDataNodes(SQLStatement sqlStatement, JdbcConnectionManager connectionManager, Collection<DataNode> dataNodes, SQLExprTableSource tableSource) {
        for (DataNode dataNode : dataNodes) {
            tableSource.setSimpleName(dataNode.getTable());
            tableSource.setSchema(dataNode.getSchema());
            String sql = sqlStatement.toString();
            try (DefaultConnection connection = connectionManager.getConnection(dataNode.getTargetName())) {
                connection.executeUpdate(sql, false);
            }
        }
    }

    public Set<DataNode> getDataNodes(TableHandler tableHandler) {
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
        return new HashSet<>(dataNodes);
    }
}
