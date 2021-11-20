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
package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowCreateTableStatement;
import io.mycat.*;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.prototypeserver.mysql.MySQLResultSet;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.List;


public class ShowCreateTableSQLHandler extends AbstractSQLHandler<SQLShowCreateTableStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowCreateTableStatement> request, MycatDataContext dataContext, Response response){
        SQLShowCreateTableStatement ast = request.getAst();
        SQLName name = ast.getName();
        if (name instanceof SQLIdentifierExpr){
            SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
            sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
            sqlPropertyExpr.setName(name.toString());
            ast.setName(sqlPropertyExpr);
        }
        List<ColumnDefPacket> showCreateTableColumns = PrototypeService.getShowCreateTableColumns();
        MySQLResultSet mySQLResultSet = MySQLResultSet.create(showCreateTableColumns);
        mySQLResultSet.setRows( showCreateTable(ast));
        return response.sendResultSet(mySQLResultSet.build());
    }
    public List<Object[]> showCreateTable(SQLShowCreateTableStatement statement) {
        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) statement.getName();
        String schemaName = SQLUtils.normalize(sqlPropertyExpr.getOwnerName());
        String tableName = SQLUtils.normalize(sqlPropertyExpr.getSimpleName());
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        String createTableSQL = tableHandler.getCreateTableSQL();
        ArrayList<Object[]> objects = new ArrayList<>();
        objects.add(new Object[]{tableName, createTableSQL});
        return objects;
    }
}
