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
package io.mycat.sqlhandler.dml;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class TruncateSQLHandler extends AbstractSQLHandler<SQLTruncateStatement> {


    @Override
    protected Future<Void> onExecute(SQLRequest<SQLTruncateStatement> request, MycatDataContext dataContext, Response response)  {
        SQLTruncateStatement truncateStatement = request.getAst();

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);


        for (SQLExprTableSource source : new ArrayList<>(truncateStatement.getTableSources())) {
            resolveSQLExprTableSource(source,dataContext);
            SQLTruncateStatement eachTruncateStatement   = clone(truncateStatement);
            eachTruncateStatement.getTableSources().clear();
            eachTruncateStatement.addTableSource(source.getName());

            TableHandler table = metadataManager.getTable(
                    SQLUtils.normalize(source.getSchema()),
                    SQLUtils.normalize(source.getTableName()));
            executeOnDataNodes(eachTruncateStatement,jdbcConnectionManager,getDataNodes(table));
        }
        return response.sendOk();
    }

    private SQLTruncateStatement clone(SQLTruncateStatement truncateStatement) {
        return (SQLTruncateStatement)SQLUtils.parseSingleMysqlStatement(truncateStatement.toString());
    }

    public void executeOnDataNodes(SQLTruncateStatement truncateStatement,
                                   JdbcConnectionManager connectionManager,
                                   Collection<DataNode> dataNodes) {
        SQLExprTableSource tableSource = truncateStatement.getTableSources().get(0);
        executeOnDataNodes(truncateStatement, connectionManager, dataNodes, tableSource);
    }

}
