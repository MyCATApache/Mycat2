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
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLShowColumnsStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;


/**
 * @ chenjunwen
 */

public class ShowColumnsSQLHandler extends AbstractSQLHandler<SQLShowColumnsStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowColumnsSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowColumnsStatement> request, MycatDataContext dataContext, Response response) {
        SQLShowColumnsStatement ast = request.getAst();
        if (ast.getDatabase() == null && dataContext.getDefaultSchema() != null) {
            ast.setDatabase(new SQLIdentifierExpr(dataContext.getDefaultSchema()));
        }
        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }

    private String toNormalSQL(SQLShowColumnsStatement ast) {

        boolean full = ast.isFull();

        SQLName table = ast.getTable();
        SQLName database = ast.getDatabase();
        SQLExpr like = ast.getLike();
        SQLExpr where = ast.getWhere();
        List<String[]> list = full ?
                Arrays.asList(
                        new String[]{"COLUMN_NAME", "Field"},
                        new String[]{"COLUMN_TYPE", "Type"},//varchar(64)
                        new String[]{"COLLATION_NAME", "Collation"},//utf8_tolower_ci
                        new String[]{"IS_NULLABLE", "Null"},//YES
                        new String[]{"COLUMN_KEY", "Key"},//""
                        new String[]{"COLUMN_DEFAULT", "Default"},//null
                        new String[]{"EXTRA", "Extra"},//""
                        new String[]{"PRIVILEGES", "Privileges"},//SELECT...
                        new String[]{"COLUMN_COMMENT", "Comment"}//""
                ) : Arrays.asList(
                new String[]{"COLUMN_NAME", "Field"},
                new String[]{"COLUMN_TYPE", "Type"},//varchar(64)
                new String[]{"COLLATION_NAME", "Collation"},//utf8_tolower_ci
                new String[]{"IS_NULLABLE", "Null"},//YES
                new String[]{"COLUMN_KEY", "Key"},//""
                new String[]{"COLUMN_DEFAULT", "Default"},//null
                new String[]{"EXTRA", "Extra"}//""
        );
        return generateSimpleSQL(list, "information_schema", "COLUMNS",
                "TABLE_NAME = '" + SQLUtils.normalize(table.getSimpleName()) + "' and " + " TABLE_SCHEMA = '" + SQLUtils.normalize(database.getSimpleName()) + "'",
                Optional.ofNullable(where).map(i -> i.toString()).orElse(null),
                Optional.ofNullable(like).map(i -> "COLUMN_NAME like " + i).orElse(null))
                .toString();
    }
}
