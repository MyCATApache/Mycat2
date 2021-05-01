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

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import io.vertx.core.Future;


import java.sql.JDBCType;
import java.util.Arrays;

/**
 * chenjunwen
 * mock ShowEngines
 */

public class ShowEnginesSQLHandler extends AbstractSQLHandler<MySqlShowEnginesStatement> {

    @Override
    protected Future<Void> onExecute(SQLRequest<MySqlShowEnginesStatement> request, MycatDataContext dataContext, Response response) {

        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();

        resultSetBuilder.addColumnInfo("Engine", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Support", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("Comment", JDBCType.VARCHAR);

        resultSetBuilder.addObjectRowPayload(Arrays.asList("InnoDB","DRFAULT","Supports transactions, row-level locking, foreign keys and encryption for tables"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("CSV","YES","Stores tables as CSV files"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("MRG_MyISAM","YES","Collection of identical MyISAM tables"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("MEMORY","YES","Hash based, stored in memory, useful for temporary tables"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("MyISAM","YES","Non-transactional engine with good performance and small data footprint"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("SEQUENCE","YES","Generated tables filled with sequential values"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("Aria","YES","Crash-safe tables with MyISAM heritage"));
        resultSetBuilder.addObjectRowPayload(Arrays.asList("PERFORMANCE_SCHEMA","YES","Performance Schema"));

        return response.sendResultSet(()->resultSetBuilder.build());
    }
}
