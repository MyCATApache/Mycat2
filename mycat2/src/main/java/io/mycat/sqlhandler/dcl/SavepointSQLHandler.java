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
package io.mycat.sqlhandler.dcl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.statement.SQLSavePointStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;


public class SavepointSQLHandler extends AbstractSQLHandler<SQLSavePointStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSavePointStatement> request, MycatDataContext dataContext, Response response) {
        SQLSavePointStatement ast = request.getAst();
        SQLExpr name = ast.getName();
        return response.setSavepoint(SQLUtils.normalize(name.toString()));
    }
}
