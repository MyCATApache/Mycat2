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
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Optional;


public class ShowDatabasesHanlder extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement> {
    public  static final SQLSelectStatement sqlSelectStatement = (SQLSelectStatement)SQLUtils.parseSingleMysqlStatement("select 1 from db1.travelrecord");
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) {
        String sql = toNormalSQL(request.getAst());
        return response.sendResultSet(runAsRowIterator(dataContext, sql));
    }


    @NotNull
    public static String toNormalSQL(SQLShowDatabasesStatement requestAst) {
        SQLExpr like = requestAst.getLike();
        SQLExpr where = requestAst.getWhere();
        return generateSimpleSQL(Collections.singletonList(new String[]{"SCHEMA_NAME","Database"}),"information_schema","SCHEMATA",
                null,
                Optional.ofNullable(where).map(i->i.toString()).orElse(null),
                Optional.ofNullable(like).map(i->"SCHEMA_NAME like "+ i).orElse(null)
                ).toString();
    }
}