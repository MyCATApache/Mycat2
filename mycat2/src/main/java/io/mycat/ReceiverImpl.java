package io.mycat;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.proxy.ResultSetProvider;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.runtime.TransactionSessionUtil;
import io.mycat.util.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Consumer;

import static io.mycat.SQLExecuterWriter.writeToMycatSession;


public class ReceiverImpl implements Response {
    static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    final MycatSession session;

    public ReceiverImpl(MycatSession session) {
        this.session = session;
    }

    @Override
    public void setHasMore(boolean more) {
        if (more) {
            sendError(new MycatException("unsupport multi statements"));
        }

    }

    @Override
    public void sendError(Throwable e) {
        session.setLastMessage(e);
        session.writeErrorEndPacketBySyncInProcessError();

    }

    @Override
    public void sendOk() {
        session.writeOkEndPacket();
    }


    @Override
    public void evalSimpleSql(SQLSelectStatement sql) {
        //没有处理的sql,例如没有替换事务状态,自动提交状态的sql,随机发到后端会返回该随机的服务器状态
        String target = session.isBindMySQLSession() ? session.getMySQLSession().getDatasource().getName() : ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
        ExplainDetail detail = ExplainDetail.builder()
                .executeType(ExecuteType.QUERY)
                .needStartTransaction(true)
                .targets(Collections.singletonMap(target, Collections.singletonList(Objects.toString(sql))))
                .build();
        execute(detail);
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        ExplainDetail detail = ExplainDetail.builder()
                .executeType(ExecuteType.QUERY)
                .needStartTransaction(true)
                .targets(Collections.singletonMap(defaultTargetName, Collections.singletonList(statement)))
                .build();
        execute(detail);
    }


    @Override
    public void proxyUpdate(String defaultTargetName, String sql) {
        ExplainDetail detail = ExplainDetail.builder()
                .executeType(ExecuteType.UPDATE)
                .needStartTransaction(true)
                .targets(Collections.singletonMap(defaultTargetName, Collections.singletonList(sql)))
                .build();
        this.execute(detail);
    }


    @Override
    public void proxyDDL(SQLStatement statement) {
        String datasourceNameByRandom = ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource();
        ExplainDetail detail = ExplainDetail.builder()
                .executeType(ExecuteType.UPDATE)
                .needStartTransaction(true)
                .targets(Collections.singletonMap(datasourceNameByRandom, Collections.singletonList(Objects.toString(statement))))
                .build();
        this.execute(detail);
    }

    @Override
    public void proxyShow(SQLStatement statement) {
        proxyDDL(statement);
    }

    @Override
    public void multiUpdate(String string, Iterator<TextUpdateInfo> apply) {
        ExplainDetail detail = ExplainDetail.builder()
                .executeType(ExecuteType.UPDATE)
                .needStartTransaction(true)
                .targets(toMap(apply))
                .build();
        this.execute(detail);
    }

    @Override
    public void multiInsert(String string, Iterator<TextUpdateInfo> apply) {
        ExplainDetail detail = ExplainDetail.builder()
                .executeType(ExecuteType.INSERT)
                .needStartTransaction(true)
                .targets(toMap(apply))
                .build();
        this.execute(detail);
    }

    @NotNull
    public Map<String, List<String>> toMap(Iterator<TextUpdateInfo> apply) {
        Map<String, List<String>> map = new HashMap<>();
        while (apply.hasNext()) {
            TextUpdateInfo next = apply.next();
            List<String> sqls = next.sqls();
            String targetName = next.targetName();
            List<String> strings = map.computeIfAbsent(targetName, s -> new ArrayList<>(1));
            strings.addAll(sqls);
        }
        return map;
    }

    @Override
    public void sendError(String errorMessage, int errorCode) {
        session.setLastMessage(errorMessage);
        session.setLastErrorCode(errorCode);
        session.writeErrorEndPacketBySyncInProcessError();
    }

    @Override
    public void sendExplain(Class defErrorCommandClass, Object map) {
        String message = Objects.toString(map);
        writePlan(session, message);
    }

    @Override
    public void sendResultSet(RowBaseIterator rowBaseIterator) {
        sendResponse(new MycatResponse[]{new TextResultSetResponse(rowBaseIterator)});
    }

    @Override
    public void sendResponse(MycatResponse[] mycatResponses) {
        SQLExecuterWriter.writeToMycatSession(session, mycatResponses);
    }

    @Override
    public void rollback() {
        MycatDataContext dataContext = session.getDataContext();
        TransactionType transactionType = dataContext.transactionType();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        switch (transactionType) {
            case PROXY_TRANSACTION_TYPE:
                transactionSession.rollback();
                if (session.isBindMySQLSession()) {
                    MySQLTaskUtil.proxyBackend(session, "ROLLBACK");
                    LOGGER.debug("session id:{} action: rollback from binding session", session.sessionId());
                    return;
                } else {
                    session.writeOkEndPacket();
                    LOGGER.debug("session id:{} action: rollback from unbinding session", session.sessionId());
                    return;
                }
            case JDBC_TRANSACTION_TYPE:
                block(mycat -> {
                    transactionSession.rollback();
                    LOGGER.debug("session id:{} action: rollback from xa", session.sessionId());
                    mycat.writeOkEndPacket();
                });
        }
    }

    @Override
    public void begin() {
        MycatDataContext dataContext = session.getDataContext();
        TransactionType transactionType = dataContext.transactionType();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        switch (transactionType) {
            case PROXY_TRANSACTION_TYPE:
                transactionSession.begin();
                LOGGER.debug("session id:{} action:{}", session.sessionId(), "begin exe success");
                session.writeOkEndPacket();
                return;
            case JDBC_TRANSACTION_TYPE:
                block(mycat -> {
                    transactionSession.begin();
                    LOGGER.debug("session id:{} action: begin from xa", session.sessionId());
                    mycat.writeOkEndPacket();
                });
        }
    }

    @Override
    public void commit() {
        MycatDataContext dataContext = session.getDataContext();
        TransactionType transactionType = dataContext.transactionType();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        switch (transactionType) {
            case PROXY_TRANSACTION_TYPE:
                transactionSession.commit();
                if (!session.isBindMySQLSession()) {
                    LOGGER.debug("session id:{} action: commit from unbinding session", session.sessionId());
                    session.writeOkEndPacket();
                    return;
                } else {
                    MySQLTaskUtil.proxyBackend(session, "COMMIT");
                    LOGGER.debug("session id:{} action: commit from binding session", session.sessionId());
                    return;
                }
            case JDBC_TRANSACTION_TYPE:
                block(mycat -> {
                    transactionSession.commit();
                    LOGGER.debug("session id:{} action: commit from xa", session.sessionId());
                    mycat.writeOkEndPacket();
                });
        }
    }

    @Override
    public void execute(ExplainDetail details) {
        MycatDataContext client = Objects.requireNonNull(session.unwrap(MycatDataContext.class));
        Map<String, List<String>> tasks = details.targets;
        String balance = details.balance;
        ExecuteType executeType = details.executeType;
        MySQLIsolation isolation = session.getIsolation();

        LOGGER.debug("session id:{} execute :{}", session.sessionId(), details.toString());

        if (details.globalTableUpdate & (client.transactionType() == TransactionType.PROXY_TRANSACTION_TYPE || details.forceProxy)) {
            executeGlobalUpdateByProxy(details);
            return;
        }
        boolean runOnProxy = isOne(tasks) && client.transactionType() == TransactionType.PROXY_TRANSACTION_TYPE || details.forceProxy;
        //return
        if (runOnProxy) {
            if (tasks.size() != 1) throw new IllegalArgumentException();
            String[] strings = checkThenGetOne(tasks);
            MySQLTaskUtil.proxyBackendByTargetName(session, strings[0], strings[1],
                    MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                    session.getIsolation(), details.executeType.isMaster(), balance);
            //return
        } else {
            block(mycat -> {
                        TransactionSession transactionSession = session.getDataContext().getTransactionSession();
                        if (details.needStartTransaction) {
                            LOGGER.debug("session id:{} startTransaction", session.sessionId());
                            // TransactionSessionUtil.reset();
                            transactionSession.setTransactionIsolation(isolation.getJdbcValue());
                            transactionSession.begin();
                            session.setInTranscation(true);
                        }
                        switch (executeType) {
                            case QUERY_MASTER:
                            case QUERY: {
                                Map<String, List<String>> backendTableInfos = details.targets;
                                String[] infos = checkThenGetOne(backendTableInfos);
                                writeToMycatSession(session, TransactionSessionUtil.executeQuery(transactionSession, infos[0], infos[1]));
                                return;
                            }
                            case INSERT:
                            case UPDATE:
                                writeToMycatSession(session, TransactionSessionUtil.executeUpdateByDatasouce(transactionSession, tasks, true, details.globalTableUpdate));
                                return;
                        }
                        throw new IllegalArgumentException();
                    }
            );
        }

    }

    public void executeGlobalUpdateByProxy(ExplainDetail details) {
        block((mycat -> {
            Map<String, List<String>> targets = details.targets;
            if (targets.isEmpty()) {
                throw new AssertionError();
            }
            int count = targets.size();
            String targetName = null;
            String sql = null;
            for (Map.Entry<String, List<String>> stringListEntry : targets.entrySet()) {
                if (count == 1) {
                    targetName = stringListEntry.getKey();
                    List<String> value = stringListEntry.getValue();
                    if (value.size() > 1) {
                        List<String> strings = value.subList(1, value.size());
                        try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(stringListEntry.getKey())) {
                            for (String s : strings) {
                                connection.executeUpdate(s, true, 0);
                            }
                        }
                    }
                    sql = value.get(0);
                    break;
                } else {
                    try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(stringListEntry.getKey())) {
                        for (String s : stringListEntry.getValue()) {
                            connection.executeUpdate(s, true, 0);
                        }
                    }
                    count--;
                }
            }
            MySQLTaskUtil.proxyBackendByTargetName(session, targetName, sql,
                    MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                    session.getIsolation(), details.executeType.isMaster(), details.balance);
        }));
    }

    public static String[] checkThenGetOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            throw new IllegalArgumentException();
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        if (list.size() != 1) {
            throw new IllegalArgumentException();
        }
        return new String[]{next.getKey(), list.get(0)};
    }

    public static boolean isOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            return false;
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        return list.size() == 1;
    }

    public void writePlan(String message) {
        writePlan(session, Collections.singletonList(message));
    }

    public static void writePlan(MycatSession session, String message) {
        writePlan(session, Collections.singletonList(message));
    }

    public static void writePlan(MycatSession session, List<String> messages) {
        MycatResultSet defaultResultSet = ResultSetProvider.INSTANCE.createDefaultResultSet(1, 33, Charset.defaultCharset());
        defaultResultSet.addColumnDef(0, "plan", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        messages.stream().map(i -> i.replaceAll("\n", " ")).forEach(defaultResultSet::addTextRowPayload);
        SQLExecuterWriter.writeToMycatSession(session, defaultResultSet);
    }

    public void block(Consumer<MycatSession> consumer) {
        if (!session.isIOThreadMode()) {
            session.getDataContext().block(() -> consumer.accept(session));
        } else {
            consumer.accept(session);
        }
    }
}