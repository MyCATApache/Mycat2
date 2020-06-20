package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.manager.ManagerCommandDispatcher;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;

public class ShowHelpCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@help";
    }

    @Override
    public String description() {
        return "show @@help";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("STATEMENT", JDBCType.VARCHAR);
        builder.addColumnInfo("DESCRIPTION", JDBCType.VARCHAR);

        ManagerCommandDispatcher.COMMANDS.forEach(i->{
            builder.addObjectRowPayload(Arrays.asList(i.statement(),i.description()));
        });
        response.sendResultSet(()->builder.build());
    }
}