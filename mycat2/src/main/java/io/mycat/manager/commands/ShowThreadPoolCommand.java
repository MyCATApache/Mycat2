package io.mycat.manager.commands;

import io.mycat.MycatDataContext;
import io.mycat.MycatWorkerProcessor;
import io.mycat.NameableExecutor;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.runtime.MycatDataContextSupport;
import io.mycat.thread.GThreadPool;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;

public class ShowThreadPoolCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@threadPool";
    }

    @Override
    public String description() {
        return "show @@threadPool";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("NAME", JDBCType.VARCHAR)
                .addColumnInfo("POOL_SIZE",JDBCType.BIGINT)
                .addColumnInfo("ACTIVE_COUNT",JDBCType.BIGINT)
                .addColumnInfo("TASK_QUEUE_SIZE",JDBCType.BIGINT)
                .addColumnInfo("COMPLETED_TASK",JDBCType.BIGINT)
                .addColumnInfo("TOTAL_TASK",JDBCType.BIGINT);
        List<NameableExecutor> nameableExecutors = Arrays.asList(MycatWorkerProcessor.INSTANCE.getMycatWorker(),
                MycatWorkerProcessor.INSTANCE.getTimeWorker());

        GThreadPool gThreadPool = MycatDataContextSupport.INSTANCE.getgThreadPool();

        int pendingSize = gThreadPool.getPendingSize();
        long completedTasks = gThreadPool.getCompletedTasks();
        builder.addObjectRowPayload(Arrays.asList(
                gThreadPool.toString(),
                gThreadPool.getMaxThread(),
                gThreadPool.getThreadCounter(),
                pendingSize,
                completedTasks,
                pendingSize+completedTasks
        ));
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
    }

}