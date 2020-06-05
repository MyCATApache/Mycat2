package io.mycat.sqlHandler.dql;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.sql.JDBCType;
import java.util.List;

@Resource
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