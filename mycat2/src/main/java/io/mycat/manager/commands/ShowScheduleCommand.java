package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.ScheduleUtil;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.concurrent.ScheduledExecutorService;

public class ShowScheduleCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@schedule";
    }

    @Override
    public String description() {
        return "show @@schedule";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        ScheduledExecutorService timer = ScheduleUtil.getTimer();
        String NAME = timer.toString();
        boolean IS_TERMINATED = timer.isTerminated();
        boolean IS_SHUTDOWN = timer.isShutdown();
        int SCHEDULE_COUNT = ScheduleUtil.getScheduleCount();
        builder.addColumnInfo("NAME", JDBCType.VARCHAR)
                .addColumnInfo("IS_TERMINATED",JDBCType.BOOLEAN)
                .addColumnInfo("IS_SHUTDOWN",JDBCType.BOOLEAN)
                .addColumnInfo("SCHEDULE_COUNT",JDBCType.BIGINT);
        builder.addObjectRowPayload(Arrays.asList(NAME,IS_TERMINATED,IS_SHUTDOWN,SCHEDULE_COUNT));
        response.sendResultSet(()->builder.build());
    }
}