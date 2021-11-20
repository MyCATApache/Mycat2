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
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.*;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.Response;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.prototypeserver.mysql.MySQLResultSet;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin.javascript.navig.Array;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


public class ShowTablesSQLHandler extends AbstractSQLHandler<SQLShowTablesStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowTablesSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowTablesStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowTablesStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        SQLName database = ast.getDatabase();
        if (database == null) {
            return response.sendError(new MycatException("NO DATABASES SELECTED"));
        }
        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }

    private String toNormalSQL(SQLShowTablesStatement ast) {

        SQLName database = ast.getDatabase();
        SQLExpr like = ast.getLike();

        // for mysql
        boolean full = ast.isFull();
        SQLExpr where = ast.getWhere();

        List<String[]> strings = full ? Arrays.asList(new String[]{"TABLE_NAME", "Tables_in_" + SQLUtils.normalize(database.getSimpleName())},
                new String[]{"TABLE_TYPE", "Table_type"}) : Collections.singletonList(new String[]{"TABLE_NAME", "Tables_in_" + SQLUtils.normalize(database.getSimpleName())});
        return generateSimpleSQL(strings, "INFORMATION_SCHEMA", "TABLES", "TABLE_SCHEMA = '" + SQLUtils.normalize(database.getSimpleName()).toLowerCase()+ "'",
                Optional.ofNullable(where).map(i -> i.toString()).orElse(null)
                ,
                Optional.ofNullable(like).map(i -> "TABLE_NAME like " + i.toString()).orElse(null)
                ).toString();
    }


}
