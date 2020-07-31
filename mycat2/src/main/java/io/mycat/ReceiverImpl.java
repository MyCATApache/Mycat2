package io.mycat;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.beans.resultset.MycatResultSet;
import io.mycat.commands.ExecuteCommand;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.TransactionSessionUtil;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.proxy.ResultSetProvider;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.resultset.BinaryResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.util.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.mycat.ExecuteType.QUERY;
import static io.mycat.ExecuteType.QUERY_MASTER;
import static io.mycat.SQLExecuterWriter.writeToMycatSession;


public class ReceiverImpl implements Response {
    static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    protected final MycatSession session;
    private boolean explainMode = false;

    public ReceiverImpl(MycatSession session) {
        this.session = session;
    }


    @Override
    public void setExplainMode(boolean bool) {
        this.explainMode = bool;
    }

    @Override
    public void setHasMore(boolean more) {
        if (more) {
            sendError(new MycatException("unsupport multi statements"));
        }

    }

    @Override
    public void sendError(Throwable e) {
        if (!this.explainMode) {
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
        } else {
            sendExplain(null, "sendError=" + e);
        }
    }

    @Override
    public void sendOk() {
        if (!this.explainMode) {
            session.writeOkEndPacket();
        } else {
            sendExplain(null, "sendOk");
        }
    }


    @Override
    public void evalSimpleSql(SQLStatement sql) {
        //没有处理的sql,例如没有替换事务状态,自动提交状态的sql,随机发到后端会返回该随机的服务器状态
        String target = session.isBindMySQLSession() ? session.getMySQLSession().getDatasource().getName() : ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByRandom();
        ExplainDetail detail = getExplainDetail(target, sql.toString(), QUERY);
        if (this.explainMode) {
            sendExplain(null, detail.toExplain());
        } else {
            if (detail.needStartTransaction) {//需要事务就开启事务
                session.getDataContext().getTransactionSession().begin();
            }
            block(session -> {
                try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(target)) {
                    try (RowBaseIterator rowBaseIterator = connection.executeQuery(sql.toString())) {
                        sendResultSet(() -> rowBaseIterator, () -> {
                            throw new UnsupportedOperationException();
                        });
                    }
                }
            });
        }
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        ExplainDetail detail = getExplainDetail(defaultTargetName, statement, QUERY);
        execute(detail);
    }


    @Override
    public void proxyUpdate(String defaultTargetName, String sql) {
        String text = sql;
        ExecuteType executeType = ExecuteType.UPDATE;
        ExplainDetail detail = getExplainDetail(defaultTargetName, text, executeType);
        this.execute(detail);
    }

    public ExplainDetail getExplainDetail(String defaultTargetName, String text, ExecuteType executeType) {
        return ExecuteCommand.getDetails(false, defaultTargetName, session.getDataContext(), null, text, executeType, false);
    }


    @Override
    public void proxyDDL(SQLStatement statement) {
        String replicaDataSource = ReplicaSelectorRuntime.INSTANCE.getPrototypeOrFirstReplicaDataSource();
        ExplainDetail detail = getExplainDetail(replicaDataSource, statement.toString(), QUERY_MASTER);
        this.execute(detail);
    }

    @Override
    public void proxyShow(SQLStatement statement) {
        proxyDDL(statement);
    }

    @Override
    public void tryBroadcast(SQLStatement statement) {
        String sql = statement.toString();
        JdbcConnectionManager connectionManager = JdbcRuntime.INSTANCE.getConnectionManager();
        List<String> infos = new ArrayList<>();
        List<String> keySet =  new ArrayList<>(connectionManager.getDatasourceInfo().keySet());
        Collections.shuffle(keySet);
        for (String datasourceName :keySet) {
            try (DefaultConnection connection = connectionManager.getConnection(datasourceName)) {
                RowBaseIterator rowBaseIterator = connection.executeQuery(sql);
                this.sendResultSet(() -> rowBaseIterator);
                return;
            } catch (Throwable e) {
                infos.add("数据源:" + datasourceName + " : " + e + "");
                continue;
            }
        }
        MycatException mycatException = new MycatException("物理分片不存在能够正确处理:\n" + statement + " \n" + String.join(",\n", infos));
        LOGGER.error("", mycatException);
        this.sendError(mycatException);
    }

    @Override
    public void multiUpdate(String string, Iterator<TextUpdateInfo> apply) {
        ExplainDetail detail = getExplainDetail(string, "", ExecuteType.UPDATE);
        detail.setTargets(toMap(apply));
        this.execute(detail);
    }

    @Override
    public void multiInsert(String string, Iterator<TextUpdateInfo> apply) {
        ExplainDetail detail = getExplainDetail(string, "", ExecuteType.INSERT);
        detail.setTargets(toMap(apply));
        this.execute(detail);
    }

    @NotNull
    public static Map<String, List<String>> toMap(Iterator<TextUpdateInfo> apply) {
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
        if (!this.explainMode) {
            session.setLastMessage(errorMessage);
            session.setLastErrorCode(errorCode);
            session.writeErrorEndPacketBySyncInProcessError();
        } else {
            sendExplain(null, "sendError:" + errorMessage + " errorCode:" + errorCode);
        }
    }

    /**
     * @param defErrorCommandClass 可空
     * @param map
     */
    @Override
    public void sendExplain(Class defErrorCommandClass, Object map) {
        if (map instanceof List) {
            writePlan(session, (List) map);
            return;
        }
        String message = defErrorCommandClass == null ? Objects.toString(map) : Objects.toString(defErrorCommandClass) + ":" + Objects.toString(map);
        writePlan(session, Arrays.asList(message.split("\n")));
    }

    public void sendResultSet(Supplier<RowBaseIterator> rowBaseIterator) {
        sendResultSet(rowBaseIterator, () -> {
            throw new UnsupportedOperationException();
        });
    }

    @Override
    public void sendResultSet(Supplier<RowBaseIterator> rowBaseIterator, Supplier<List<String>> explainSupplier) {
        if (!this.explainMode) {
            sendResponse(new MycatResponse[]{new TextResultSetResponse(rowBaseIterator.get())}, explainSupplier);
        } else {
            sendExplain(null, explainSupplier.get());
        }
    }

    @Override
    public void sendResponse(MycatResponse[] mycatResponses, Supplier<List<String>> explainSupplier) {
        if (!this.explainMode) {
            SQLExecuterWriter.writeToMycatSession(session, mycatResponses);
        } else {
            sendExplain(null, explainSupplier.get());
        }
    }

    @Override
    public void rollback() {
        if (this.explainMode) {
            sendExplain(null, "rollback");
            return;
        }
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
        if (this.explainMode) {
            sendExplain(null, "begin");
            return;
        }
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
        if (this.explainMode) {
            sendExplain(null, "commit");
            return;
        }
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
        boolean master = details.needStartTransaction || session.isInTransaction() || !session.isAutocommit() || details.globalTableUpdate || details.executeType.isMaster();
        MycatDataContext client = Objects.requireNonNull(session.unwrap(MycatDataContext.class));
        Map<String, List<String>> tasks = resolveDataSourceName(details.getBalance(), master, Objects.requireNonNull(details.targets));
        details.setTargets(tasks);
        ExecuteType executeType = details.executeType;
        if (this.explainMode) {
            sendExplain(null, "execute:" + details);
            return;
        }
        TransactionSession transactionSession = session.getDataContext().getTransactionSession();
        transactionSession.doAction();
        boolean runOnProxy = (client.transactionType() == TransactionType.PROXY_TRANSACTION_TYPE || details.forceProxy);
        boolean globalTableUpdate = details.globalTableUpdate;
        if (globalTableUpdate && runOnProxy) {
            executeGlobalUpdateByProxy(details);
            return;
        }
        Optional<String[]> maybe = checkThenGetOne(tasks);
        if (maybe.isPresent()) {
            String[] strings = maybe.get();
            String datasourceName = strings[0];
            String sql = strings[1];
            if (runOnProxy && MycatDatasourceUtil.isProxyDatasource(datasourceName)) {
                MySQLTaskUtil.proxyBackendByDatasourceName(session, datasourceName, sql,
                        MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                        session.getIsolation());
                return;
            }
            if ((executeType == QUERY_MASTER || executeType == QUERY)) {
                block(mycat -> {
                    switch (executeType) {
                        case QUERY_MASTER:
                        case QUERY: {
                            MycatConnection connection = transactionSession.getConnection(datasourceName);
                            TextResultSetResponse textResultSetResponse = new TextResultSetResponse(connection.executeQuery(null, sql));
                            writeToMycatSession(session, textResultSetResponse);
                            return;
                        }
                        default:
                            throw new IllegalStateException("Unexpected value: " + executeType);
                    }
                });
                return;
            }
        }
        block(mycat -> {
                    switch (executeType) {
                        case INSERT:
                        case UPDATE:
                            UpdateRowIteratorResponse updateRowIteratorResponse = TransactionSessionUtil.executeUpdateByDatasouce(transactionSession, tasks, true, globalTableUpdate);
                            client.setLastInsertId(updateRowIteratorResponse.getLastInsertId());
                            writeToMycatSession(session, updateRowIteratorResponse);
                            return;
                        default:
                    }
                    throw new IllegalArgumentException();
                }
        );
    }

    @Override
    public void multiGlobalInsert(String string, Iterator<TextUpdateInfo> apply) {
        ExplainDetail detail = getExplainDetail(string, "", ExecuteType.INSERT);
        detail.globalTableUpdate = true;
        detail.setTargets(toMap(apply));
        this.execute(detail);
    }

    @Override
    public void multiGlobalUpdate(String string, Iterator<TextUpdateInfo> apply) {
        ExplainDetail detail = getExplainDetail(string, "", ExecuteType.UPDATE);
        detail.globalTableUpdate = true;
        detail.setTargets(toMap(apply));
        this.execute(detail);
    }

    @Override
    public void sendBinaryResultSet(Supplier<RowBaseIterator> rowBaseIterator) {
        sendResponse(new MycatResponse[]{new BinaryResultSetResponse(rowBaseIterator.get())}, null);
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
            if (MycatDatasourceUtil.isProxyDatasource(targetName)) {
                MySQLTaskUtil.proxyBackendByDatasourceName(session, targetName, sql,
                        MySQLTaskUtil.TransactionSyncType.create(session.isAutocommit(), session.isInTransaction()),
                        session.getIsolation());
                return;
            }
            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(targetName)) {
                UpdateRowIteratorResponse updateRowIteratorResponse = connection.executeUpdate(sql, true, 0);
                writeToMycatSession(session, updateRowIteratorResponse);
            }
        }));
    }

    public static Optional<String[]> checkThenGetOne(Map<String, List<String>> backendTableInfos) {
        if (backendTableInfos.size() != 1) {
            return Optional.empty();
        }
        Map.Entry<String, List<String>> next = backendTableInfos.entrySet().iterator().next();
        List<String> list = next.getValue();
        if (list.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(new String[]{next.getKey(), list.get(0)});
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
            session.getDataContext().block(() -> {
                try {
                    consumer.accept(session);
                }catch (Throwable e){
                    session.setLastMessage(e);
                    session.writeErrorEndPacketBySyncInProcessError();
                }
            });
        } else {
            consumer.accept(session);
        }
    }

    @NotNull
    private static HashMap<String, List<String>> resolveDataSourceName(String balance, boolean master, Map<String, List<String>> routeMap) {
        HashMap<String, List<String>> map = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : routeMap.entrySet()) {
            String datasourceNameByReplicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(entry.getKey(), master, balance);
            List<String> list = map.computeIfAbsent(datasourceNameByReplicaName, s -> new ArrayList<>(1));
            list.addAll(entry.getValue());
        }
        return map;
    }
}