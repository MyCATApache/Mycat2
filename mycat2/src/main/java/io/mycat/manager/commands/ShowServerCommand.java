package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;

public class ShowServerCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@server";
    }

    @Override
    public String description() {
        return statement();
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        int AVAILABLE_PROCESSORS = runtime.availableProcessors();
        long FREE_MEMORY = runtime.freeMemory();
        long TOTAL_MEMORY = runtime.totalMemory();
        long USE_MEMORY = runtime.totalMemory() - runtime.freeMemory();
        long MAX_MEMORY = runtime.maxMemory();
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("AVAILABLE_PROCESSORS", JDBCType.BIGINT)
                .addColumnInfo("MAX_MEMORY", JDBCType.BIGINT)
                .addColumnInfo("USE_MEMORY",JDBCType.BIGINT)
                .addColumnInfo("FREE_MEMORY", JDBCType.BIGINT)
                .addColumnInfo("TOTAL_MEMORY", JDBCType.BIGINT);
        builder.addObjectRowPayload(Arrays.asList(
                AVAILABLE_PROCESSORS, MAX_MEMORY,USE_MEMORY, FREE_MEMORY, TOTAL_MEMORY
        ));
        response.sendResultSet(() -> builder.build());
    }
}