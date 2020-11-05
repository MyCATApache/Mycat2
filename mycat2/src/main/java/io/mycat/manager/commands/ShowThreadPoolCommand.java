package io.mycat.manager.commands;

import io.mycat.*;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.proxy.session.MycatContextThreadPool;
import io.mycat.thread.SimpleMycatContextBindingThreadPool;
import io.mycat.util.Response;
import org.jetbrains.annotations.NotNull;

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
    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        ResultSetBuilder builder = getResultSet();
        response.sendResultSet(() -> builder.build());
    }

    @NotNull
    public static ResultSetBuilder getResultSet() {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("NAME", JDBCType.VARCHAR)
                .addColumnInfo("POOL_SIZE", JDBCType.BIGINT)
                .addColumnInfo("ACTIVE_COUNT", JDBCType.BIGINT)
                .addColumnInfo("TASK_QUEUE_SIZE", JDBCType.BIGINT)
                .addColumnInfo("COMPLETED_TASK", JDBCType.BIGINT)
                .addColumnInfo("TOTAL_TASK", JDBCType.BIGINT);

        MycatWorkerProcessor mycatWorkerProcessor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
        List<NameableExecutor> nameableExecutors = Arrays.asList(mycatWorkerProcessor.getMycatWorker(),
                mycatWorkerProcessor.getTimeWorker());
        MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);
        MycatContextThreadPool gThreadPool = mycatServer.getMycatContextThreadPool();

        //@todo
//        int pendingSize = gThreadPool.getPendingSize();
//        long completedTasks = gThreadPool.getCompletedTasks();
//        builder.addObjectRowPayload(Arrays.asList(
//                gThreadPool.toString(),
//                gThreadPool.getMaxThread(),
//                gThreadPool.getThreadCounter(),
//                pendingSize,
//                completedTasks,
//                pendingSize + completedTasks
//        ));
        for (NameableExecutor w : nameableExecutors) {
            builder.addObjectRowPayload(Arrays.asList(
                    w.getName(),
                    w.getPoolSize(),
                    w.getActiveCount(),
                    w.getQueue().size(),
                    w.getCompletedTaskCount(),
                    w.getTaskCount()));
        }
        return builder;
    }

}