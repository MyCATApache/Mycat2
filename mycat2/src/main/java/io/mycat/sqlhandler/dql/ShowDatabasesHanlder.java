package io.mycat.sqlhandler.dql;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.MetadataManager;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;


import java.sql.JDBCType;
import java.util.List;


public class ShowDatabasesHanlder extends AbstractSQLHandler<com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement> {
    @Override
    protected void onExecute(SQLRequest<com.alibaba.druid.sql.ast.statement.SQLShowDatabasesStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<String> collect = metadataManager.showDatabases();
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("Database", JDBCType.VARCHAR);
        for (String s : collect) {
            resultSetBuilder.addObjectRowPayload(s);
        }
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        response.sendResultSet(()->rowBaseIterator);
    }
}