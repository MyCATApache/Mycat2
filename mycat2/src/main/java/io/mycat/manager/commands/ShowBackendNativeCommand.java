package io.mycat.manager.commands;

import io.mycat.MycatCore;
import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Optional;

public class ShowBackendNativeCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@backend.native";
    }

    @Override
    public String description() {
        return "show the mysql session in mycat2 proxy";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("SESSION_ID", JDBCType.BIGINT)
                .addColumnInfo("THREAD_NAME",JDBCType.VARCHAR)
                .addColumnInfo("THREAD_ID",JDBCType.BIGINT)
                .addColumnInfo("DS_NAME",JDBCType.VARCHAR)
                .addColumnInfo("LAST_MESSAGE",JDBCType.VARCHAR)
                .addColumnInfo("MYCAT_SESSION_ID",JDBCType.BIGINT)
                .addColumnInfo("IS_IDLE",JDBCType.BOOLEAN)
                .addColumnInfo("SELECT_LIMIT",JDBCType.BIGINT)
                .addColumnInfo("IS_AUTOCOMMIT",JDBCType.BOOLEAN)
                .addColumnInfo("IS_RESPONSE_FINISHED",JDBCType.BOOLEAN)
                .addColumnInfo("RESPONSE_TYPE",JDBCType.VARCHAR)
                .addColumnInfo("IS_IN_TRANSACTION",JDBCType.BOOLEAN)
                .addColumnInfo("IS_REQUEST_SUCCESS",JDBCType.BOOLEAN)
                .addColumnInfo("IS_READ_ONLY",JDBCType.BOOLEAN);
        for (MycatReactorThread i : MycatCore.INSTANCE.getReactorManager().getList()) {
            MySQLSessionManager c = i.getMySQLSessionManager();
            for (MySQLClientSession session : c.getAllSessions()) {

                int SESSION_ID = session.sessionId();
                String THREAD_NAME = i.getName();
                long THREAD_ID = i.getId();
                String DS_NAME = session.getDatasource().getName();
                String LAST_MESSAGE = session.getLastMessage();
                Integer MYCAT_SESSION_ID = Optional.ofNullable(session.getMycat()).map(m->m.sessionId()).orElse(null);
                boolean IS_IDLE = session.isIdle();

                long SELECT_LIMIT = session.getSelectLimit();
                boolean IS_AUTOCOMMIT = session.isAutomCommit() == MySQLAutoCommit.ON;
                boolean IS_RESPONSE_FINISHED = session.isResponseFinished();
                String RESPONSE_TYPE =  Optional.ofNullable(session.getResponseType()).map(j->j.name()).orElse(null);
                boolean IS_IN_TRANSACTION = session.isMonopolizedByTransaction();
                boolean IS_REQUEST_SUCCESS = session.isRequestSuccess();
                boolean IS_READ_ONLY = session.isReadOnly();

                resultSetBuilder.addObjectRowPayload(Arrays.asList(
                        SESSION_ID,
                        THREAD_NAME,
                        THREAD_ID,
                        DS_NAME,
                        LAST_MESSAGE,
                        MYCAT_SESSION_ID,
                        IS_IDLE,
                        SELECT_LIMIT,
                        IS_AUTOCOMMIT,
                        IS_RESPONSE_FINISHED,
                        RESPONSE_TYPE,
                        IS_IN_TRANSACTION,
                        IS_REQUEST_SUCCESS,
                        IS_READ_ONLY
                ));
            }

        }
        response.sendResultSet(()->resultSetBuilder.build());
    }
}