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
package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement;
import io.mycat.*;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import io.vertx.core.shareddata.Lock;

import java.util.Collections;


public class DropSequenceSQLHandler extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLDropSequenceStatement> request, MycatDataContext dataContext, Response response) {
        LockService lockService = MetaClusterCurrent.wrapper(LockService.class);
        Future<Lock> lockFuture = lockService.getLock(DDL_LOCK);
        return lockFuture.flatMap(lock -> {
            try {
                SQLDropSequenceStatement ast = request.getAst();
                SQLName name = ast.getName();
                if (name instanceof SQLIdentifierExpr) {
                    SQLPropertyExpr sqlPropertyExpr = new SQLPropertyExpr();
                    sqlPropertyExpr.setOwner(dataContext.getDefaultSchema());
                    sqlPropertyExpr.setName(name.toString());
                    ast.setName(sqlPropertyExpr);
                }
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                return response.proxyUpdate(Collections.singletonList(metadataManager.getPrototype()), ast.toString(),Collections.emptyList());
            }catch (Throwable throwable){
                return Future.failedFuture(throwable);
            }finally {
                lock.release();
            }
        });

    }
}
