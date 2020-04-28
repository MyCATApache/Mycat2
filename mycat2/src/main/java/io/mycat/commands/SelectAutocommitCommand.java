package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.SQLRequest;
import io.mycat.util.Response;

import java.sql.JDBCType;

public class SelectAutocommitCommand implements MycatCommand{
    String columnName = "@@session.autocommit";

    @Override
    public boolean run(SQLRequest request, MycatDataContext context, Response response) {
        int isAutocommit = context.isAutocommit() ? 1 :0;
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo(columnName, JDBCType.BIGINT);
        resultSetBuilder.addObjectRowPayload(isAutocommit);
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        response.sendResultSet(rowBaseIterator);
        return true;
    }

    @Override
    public boolean explain(SQLRequest request, MycatDataContext context, Response response) {
         response.sendExplain(SelectAutocommitCommand.class,columnName+":"+ (context.isAutocommit() ? 1 :0));
         return true;
    }

    @Override
    public String getName() {
        return "selectAutocommit";
    }
}