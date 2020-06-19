package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;

public class ShowThreadPoolCommand implements MycatCommand {
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if ("show @@threadPool".equalsIgnoreCase(request.getText())){
            ResultSetBuilder builder = ResultSetBuilder.create();
            builder.addColumnInfo("NAME", JDBCType.VARCHAR)
                    .addColumnInfo("POOL_SIZE",JDBCType.BIGINT)
                    .addColumnInfo("ACTIVE_COUNT",JDBCType.BIGINT)
                    .addColumnInfo("TASK_QUEUE_SIZE",JDBCType.BIGINT)
                    .addColumnInfo("COMPLETED_TASK",JDBCType.BIGINT)
                    .addColumnInfo("TOTAL_TASK",JDBCType.BIGINT);
            List<NameableExecutor> nameableExecutors = Arrays.asList(MycatWorkerProcessor.INSTANCE.getMycatWorker(),
                    MycatWorkerProcessor.INSTANCE.getTimeWorker());
            for (NameableExecutor w : nameableExecutors) {
                builder.addObjectRowPayload(Arrays.asList(
                        w.getName(),
                        w.getPoolSize(),
                        w.getActiveCount(),
                        w.getQueue().size(),
                        w.getCompletedTaskCount(),
                        w.getTaskCount()));
            }
            response.sendResultSet(()->builder.build());
            return true;
        }
        return false;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        return false;
    }

    @Override
    public String getName() {
        return getClass().getName();
    }
}