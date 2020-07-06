package io.mycat.sqlhandler.dql;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;


import java.sql.JDBCType;
import java.util.List;


public class ShowDatabasesHanlder extends AbstractSQLHandler<com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement> {
    @Override
    protected ExecuteCode onExecute(SQLRequest<com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) {
        List<String> collect = MetadataManager.INSTANCE.showDatabases();
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("Database", JDBCType.VARCHAR);
        for (String s : collect) {
            resultSetBuilder.addObjectRowPayload(s);
        }
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        response.sendResultSet(()->rowBaseIterator, () -> {
            throw new UnsupportedOperationException();
        });
        return ExecuteCode.PERFORMED;
    }
}