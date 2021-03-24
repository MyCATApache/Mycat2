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

import com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


import java.sql.JDBCType;
import java.util.List;


public class ShowDatabasesHanlder extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement> {
    @Override
    protected Future<Void> onExecute(SQLRequest<SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<String> collect = metadataManager.showDatabases();
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("Database", JDBCType.VARCHAR);
        for (String s : collect) {
            resultSetBuilder.addObjectRowPayload(s);
        }
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        return response.sendResultSet(()->rowBaseIterator);
    }
}