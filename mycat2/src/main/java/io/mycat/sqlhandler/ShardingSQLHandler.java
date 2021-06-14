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
package io.mycat.sqlhandler;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.*;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.util.Pair;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    private final static Logger LOGGER = LoggerFactory.getLogger(ShardingSQLHandler.class);
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response){
        HackRouter hackRouter = new HackRouter(request.getAst(), dataContext);
        try {
            if (hackRouter.analyse()) {
                Pair<String, String> plan = hackRouter.getPlan();
                return response.proxySelect(Collections.singletonList(plan.getKey()),plan.getValue());
            } else {
               return DrdsRunnerHelper.runOnDrds(dataContext,request.getAst(),response);
            }
        }catch (Throwable throwable){
            LOGGER.error(request.getAst().toString(),throwable);
            return Future.failedFuture(throwable);
        }

    }
}