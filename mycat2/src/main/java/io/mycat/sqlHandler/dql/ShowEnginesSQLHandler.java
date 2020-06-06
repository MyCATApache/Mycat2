package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.sql.JDBCType;
import java.util.Arrays;

/**
 * chenjunwen
 * mock ShowEngines
 */
@Resource
public class ShowEnginesSQLHandler extends AbstractSQLHandler<MySqlShowEnginesStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlShowEnginesStatement> request, MycatDataContext dataContext, Response response) {

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

        response.sendResultSet(()->resultSetBuilder.build(),()->{throw new UnsupportedOperationException();});
        return ExecuteCode.PERFORMED;
    }
}
