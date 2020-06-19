package io.mycat.manager.commands;

import io.mycat.MycatCore;
import io.mycat.MycatDataContext;
import io.mycat.MycatUser;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.ReactorThreadManager;
import io.mycat.proxy.session.*;
import io.mycat.util.Dumper;
import io.mycat.util.Response;
import org.apache.zookeeper.Op;

import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ShowConnectionCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@connection";
    }

    @Override
    public String description() {
        return "show the (front) mycat session info";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ReactorThreadManager reactorManager = MycatCore.INSTANCE.getReactorManager();
        Objects.requireNonNull(reactorManager);
        List<MycatSession> sessions = reactorManager.getList().stream().flatMap(i -> i.getFrontManager().getAllSessions().stream()).collect(Collectors.toList());

        ResultSetBuilder builder = ResultSetBuilder.create();

        builder.addColumnInfo("ID", JDBCType.BIGINT);
        builder.addColumnInfo("USER_NAME", JDBCType.VARCHAR);
        builder.addColumnInfo("HOST", JDBCType.VARCHAR);
        builder.addColumnInfo("SCHEMA", JDBCType.VARCHAR);
        builder.addColumnInfo("AFFECTED_ROWS", JDBCType.BIGINT);
        builder.addColumnInfo("AUTOCOMMIT", JDBCType.BOOLEAN);
        builder.addColumnInfo("IN_TRANSACTION", JDBCType.BOOLEAN);
        builder.addColumnInfo("CHARSET", JDBCType.VARCHAR);
        builder.addColumnInfo("CHARSET_INDEX", JDBCType.BIGINT);
        builder.addColumnInfo("OPEN", JDBCType.BOOLEAN);
        builder.addColumnInfo("SERVER_CAPABILITIES", JDBCType.BIGINT);
        builder.addColumnInfo("ISOLATION", JDBCType.VARCHAR);
        builder.addColumnInfo("LAST_ERROR_CODE", JDBCType.BIGINT);
        builder.addColumnInfo("LAST_INSERT_ID", JDBCType.BIGINT);
        builder.addColumnInfo("LAST_MESSAGE", JDBCType.VARCHAR);
        builder.addColumnInfo("PROCESS_STATE", JDBCType.VARCHAR);
        builder.addColumnInfo("WARNING_COUNT", JDBCType.BIGINT);
        builder.addColumnInfo("MYSQL_SESSION_ID", JDBCType.BIGINT);
        builder.addColumnInfo("TRANSACTION_TYPE", JDBCType.VARCHAR);
        builder.addColumnInfo("TRANSCATION_SNAPSHOT", JDBCType.VARCHAR);
        builder.addColumnInfo("CANCEL_FLAG", JDBCType.BOOLEAN);

        for (MycatSession session : sessions) {
            int ID = session.sessionId();
            MycatUser user = session.getUser();
            String USER_NAME = user.getUserName();
            String HOST = user.getHost();
            String SCHEMA = session.getSchema();
            long AFFECTED_ROWS = session.getAffectedRows();
            boolean AUTOCOMMIT = session.isAutocommit();
            boolean IN_TRANSACTION = session.isInTransaction();
            String CHARSET = Optional.ofNullable(session.charset()).map(i->i.displayName()).orElse("");
            int CHARSET_INDEX = session.charsetIndex();
            boolean OPEN = session.checkOpen();
            int SERVER_CAPABILITIES = session.getCapabilities();
            String ISOLATION = session.getIsolation().getText();
            int LAST_ERROR_CODE = session.getLastErrorCode();
            long LAST_INSERT_ID = session.getLastInsertId();
            String LAST_MESSAGE = session.getLastMessage();
            String PROCESS_STATE = session.getProcessState().name();

            int WARNING_COUNT = session.getWarningCount();
            Integer MYSQL_SESSION_ID = Optional.ofNullable(session.getMySQLSession()).map(i->i.sessionId()).orElse(null);

            MycatDataContext dataContext = session.getDataContext();
            String TRANSACTION_TYPE = Optional.ofNullable(dataContext.transactionType()).map(i->i.getName()).orElse("");

            TransactionSession transactionSession = dataContext.getTransactionSession();
            String TRANSCATION_SMAPSHOT = transactionSession.snapshot().toString("|");
            boolean CANCEL_FLAG = dataContext.getCancelFlag().get();
            builder.addObjectRowPayload(Arrays.asList(
                    ID,
                    USER_NAME,
                    HOST,
                    SCHEMA,
                    AFFECTED_ROWS,
                    AUTOCOMMIT,
                    IN_TRANSACTION,
                    CHARSET,
                    CHARSET_INDEX,
                    OPEN,
                    SERVER_CAPABILITIES,
                    ISOLATION,
                    LAST_ERROR_CODE,
                    LAST_INSERT_ID,
                    LAST_MESSAGE,
                    PROCESS_STATE,
                    WARNING_COUNT,
                    MYSQL_SESSION_ID,
                    TRANSACTION_TYPE,
                    TRANSCATION_SMAPSHOT,
                    CANCEL_FLAG
            ));
        }
        response.sendResultSet(()->builder.build());
    }

}