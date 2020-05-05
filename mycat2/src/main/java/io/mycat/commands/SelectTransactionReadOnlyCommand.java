package io.mycat.commands;

import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
/**
 * @author Junwen Chen
 **/
public enum SelectTransactionReadOnlyCommand implements MycatCommand{
    INSTANCE;
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        String columnName = request.get("columnName");
        long isReadOnly = context.isReadOnly()?1:0;
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo(columnName, JDBCType.BIGINT);
        resultSetBuilder.addObjectRowPayload(isReadOnly);
        RowBaseIterator rowBaseIterator = resultSetBuilder.build();
        response.sendResultSet(rowBaseIterator, new Supplier<List<String>>() {
            @Override
            public List<String> get() {
                return Arrays.asList(""+columnName+":"+(context.isReadOnly()?1:0));
            }
        });
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        String columnName = request.get("columnName");
        response.sendExplain(SelectTransactionReadOnlyCommand.class,columnName+":"+(context.isReadOnly()?1:0));
        return true;
    }

    @Override
    public String getName() {
        return "selectTransactionReadOnly";
    }
}