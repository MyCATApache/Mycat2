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

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;

import java.util.Arrays;
import java.util.Optional;


public class ShowCollationSQLHandler extends AbstractSQLHandler<MySqlShowCollationStatement> {


    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowCollationStatement> request, MycatDataContext dataContext, Response response) {
        MySqlShowCollationStatement mySqlShowCollationStatement = request.getAst();

        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }

    private String toNormalSQL(MySqlShowCollationStatement ast) {

        SQLExpr where = ast.getWhere();
        SQLExpr pattern = ast.getPattern();

        return generateSimpleSQL(Arrays.asList(
                        new String[]{"COLLATION_NAME","Collection"},//utf8_general_ci
                        new String[]{"CHARACTER_SET_NAME","Charset"},//utf8
                        new String[]{"ID","Id"},//209
                        new String[]{"IS_DEFAULT","Default"},//YES
                        new String[]{"IS_COMPILED","Complied"},//YES
                        new String[]{"SORTLEN","Sortlen"},//8
                        new String[]{"PAD_ATTRIBUTE","Pad_attribute"}//PAD SPACE
                ),
                "information_schema","Collations",
                Optional.ofNullable(where).map(i->i.toString()).orElse(null),
                Optional.ofNullable(pattern).map(i->"COLLATION_NAME like "+i.toString()).orElse(null)).toString();
    }
}
