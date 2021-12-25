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

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.PrepareExecutor;
import io.mycat.swapbuffer.MySQLSwapbufferBuilder;
import io.mycat.swapbuffer.PacketRequest;
import io.mycat.util.Pair;
import io.ordinate.engine.util.ResultWriterUtil;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    private final static Logger LOGGER = LoggerFactory.getLogger(ShardingSQLHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) {
        Optional<Future<Void>> op = Optional.empty();
        if (dataContext.isDebug()) {
            op = testExample(request, dataContext, response);
            if (op.isPresent()) return op.get();
        }
        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(request.getAst(), dataContext.getDefaultSchema());
        HackRouter hackRouter = new HackRouter(drdsSqlWithParams.getParameterizedStatement(), dataContext);
        try {
            if (hackRouter.analyse()) {
                Pair<String, String> plan = hackRouter.getPlan();
                return response.proxySelect(Collections.singletonList(plan.getKey()), plan.getValue(),drdsSqlWithParams.getParams());
            } else {
                return DrdsRunnerHelper.runOnDrds(dataContext, drdsSqlWithParams, response);
            }
        } catch (Throwable throwable) {
            LOGGER.error(request.getAst().toString(), throwable);
            return Future.failedFuture(throwable);
        }

    }

    private Optional<Future<Void>> testExample(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) {
        String sqlString = request.getSqlString();
        if (sqlString.equalsIgnoreCase("select swapbuffer")) {
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
            resultSetBuilder.addColumnInfo("1", JDBCType.INTEGER);
            resultSetBuilder.addObjectRowPayload(Arrays.asList(1, 2));
            resultSetBuilder.addObjectRowPayload(Arrays.asList(3, 4));
            MySQLSwapbufferBuilder mySQLSwapbufferBuilder = new MySQLSwapbufferBuilder(resultSetBuilder.build());
            Observable<PacketRequest> sender = mySQLSwapbufferBuilder.build();
            return Optional.of(response.swapBuffer(sender));
        }
        if (sqlString.equalsIgnoreCase("select arrow")) {
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
            resultSetBuilder.addColumnInfo("1", JDBCType.INTEGER);
            resultSetBuilder.addObjectRowPayload(Arrays.asList(1, 2));
            resultSetBuilder.addObjectRowPayload(Arrays.asList(3, 4));
            RowBaseIterator rowBaseIterator = resultSetBuilder.build();
            Observable<VectorSchemaRoot> observable = ResultWriterUtil.convertToVector(resultSetBuilder.build());
            return Optional.of(response.sendVectorResultSet(rowBaseIterator.getMetaData(),observable));
        }
        return Optional.empty();
    }
}