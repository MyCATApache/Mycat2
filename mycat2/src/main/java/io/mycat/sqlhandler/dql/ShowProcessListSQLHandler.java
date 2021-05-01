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

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import io.mycat.MycatDataContext;
import io.mycat.Process;
import io.mycat.Response;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.Set;


public class ShowProcessListSQLHandler extends AbstractSQLHandler<MySqlShowProcessListStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowProcessListStatement> request, MycatDataContext dataContext, Response response) {
        Set<Process> processList = Process.getProcessList();
        for (Process process : processList) {
            byte[] bytes = process.toBytes();
//            response.sendResultSet()
        }
//        response.sendResultSet()
        return response.proxySelectToPrototype(request.getAst().toString());
    }
}
