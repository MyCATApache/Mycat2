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
import io.mycat.MycatUser;
import io.mycat.Process;
import io.mycat.Response;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;


public class ShowProcessListSQLHandler extends AbstractSQLHandler<MySqlShowProcessListStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowProcessListStatement> request, MycatDataContext dataContext, Response response) {
        Map<Thread, Process> processMap = new LinkedHashMap<>(Process.getProcessMap());
        MySqlShowProcessListStatement ast = request.getAst();
        boolean full = ast.isFull();
        int maxCount = full ? Integer.MAX_VALUE : 100;
        MycatUser currentUser = full ? null : dataContext.getUser();

        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("Id", JDBCType.INTEGER);
        resultSetBuilder.addColumnInfo("User", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Host", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("db", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Command", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Time", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("State", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Info", JDBCType.VARCHAR);

        long timestamp = System.currentTimeMillis();
        int currentCount = 0;
        for (Map.Entry<Thread, Process> entry : processMap.entrySet()) {
            Thread holdThread = entry.getKey();
            Process process = entry.getValue();
            if (currentUser != null && !Objects.equals(process.getUser(), currentUser.getUserName())) {
                continue;
            }
            resultSetBuilder.addObjectRowPayload(Arrays.asList(
                    process.getId(),
                    process.getUser(),
                    process.getHost(),
                    process.getDb(),
                    process.getCommand(),
                    timestamp - process.getCreateTimestamp().getTime(),
                    process.getState(),
                    process.getInfo()
            ));
            currentCount++;
            if (currentCount >= maxCount) {
                break;
            }
        }
        return response.sendResultSet(resultSetBuilder.build());
//        return response.proxySelectToPrototype(request.getAst().toString());
    }
}
