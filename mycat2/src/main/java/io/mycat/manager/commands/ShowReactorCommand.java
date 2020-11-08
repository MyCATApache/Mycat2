package io.mycat.manager.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatCore;
import io.mycat.MycatDataContext;
import io.mycat.MycatServer;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.buffer.BufferPool;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.Session;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;

public class ShowReactorCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@reactor";
    }

    @Override
    public String description() {
        return "show @@reactor";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) throws Exception {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("THREAD_NAME", JDBCType.VARCHAR)
                .addColumnInfo("THREAD_ID",JDBCType.BIGINT)
                .addColumnInfo("CUR_SESSION_ID",JDBCType.BIGINT)
                .addColumnInfo("PREPARE_STOP",JDBCType.BOOLEAN)
                .addColumnInfo("BUFFER_POOL_SNAPSHOT",JDBCType.VARCHAR)
                .addColumnInfo("LAST_ACTIVE_TIME",JDBCType.TIMESTAMP);
        MycatServer mycatServer = MetaClusterCurrent.wrapper(MycatServer.class);
        for (MycatReactorThread mycatReactorThread : mycatServer.getReactorManager().getList()) {
            String THREAD_NAME = mycatReactorThread.getName();
            long THREAD_ID = mycatReactorThread.getId();
            Integer CUR_SESSION_ID = Optional.ofNullable(mycatReactorThread.getCurSession()).map(i->i.sessionId()).orElse(null);
            boolean PREPARE_STOP = mycatReactorThread.isPrepareStop();
            String BUFFER_POOL_SNAPSHOT= Optional.ofNullable(mycatReactorThread.getBufPool()).map(i -> i.snapshot().toString("|")).orElse("");
            Timestamp LAST_ACTIVE_TIME  = new Timestamp( mycatReactorThread.getLastActiveTime());
            resultSetBuilder.addObjectRowPayload(Arrays.asList(
                    THREAD_NAME,
                    THREAD_ID,
                    CUR_SESSION_ID,
                    PREPARE_STOP,
                    BUFFER_POOL_SNAPSHOT,
                    LAST_ACTIVE_TIME
            ));
        }
        response.sendResultSet(()->resultSetBuilder.build());
    }
}