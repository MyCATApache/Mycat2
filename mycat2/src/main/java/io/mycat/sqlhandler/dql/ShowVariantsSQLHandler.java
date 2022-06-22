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
import com.alibaba.druid.sql.ast.statement.SQLShowVariantsStatement;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

/**
 * chenjunwen
 * 实现ShowVariants
 */

public class ShowVariantsSQLHandler extends AbstractSQLHandler<SQLShowVariantsStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowVariantsSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowVariantsStatement> request, MycatDataContext dataContext, Response response) {

        SQLShowVariantsStatement requestAst = request.getAst();

        if (!requestAst.isGlobal() && !requestAst.isSession()) {
            requestAst.setSession(true);
            requestAst.setGlobal(false);
        }
        if (requestAst.isGlobal() && requestAst.isSession()) {
            requestAst.setSession(true);
            requestAst.setGlobal(false);
        }
        String sql = toNormalSQL(requestAst);
        return DrdsRunnerHelper.runOnDrds(dataContext, DrdsRunnerHelper.preParse(sql, dataContext.getDefaultSchema()), response);
    }

    private String toNormalSQL(SQLShowVariantsStatement ast) {


        SQLExpr like = ast.getLike();
        SQLExpr where = ast.getWhere();

        String tableName = ast.isSession() ? "session_variables" : "global_variables";

        return generateSimpleSQL(
                Arrays.asList(
                new String[]{"VARIABLE_NAME","Variable_name"},
                new String[]{"VARIABLE_VALUE","Value"}),
                "information_schema", tableName, Optional.ofNullable(where).map(i->i.toString()).orElse(null),
                Optional.ofNullable(like).map(i->"Variable_name like "+i).orElse(null)).toString();
    }


}
