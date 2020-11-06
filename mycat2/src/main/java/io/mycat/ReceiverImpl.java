package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.resultset.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.metadata.MetadataManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.Response;
import org.apache.calcite.avatica.proto.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.mycat.ExecuteType.*;


public class ReceiverImpl implements Response {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    protected final MycatSession session;
    protected final SQLExecuterWriter sqlExecuterWriter;

    public ReceiverImpl(MycatSession session,int stmtSize, boolean binary,boolean explain) {
        this.sqlExecuterWriter = new SQLExecuterWriter(stmtSize, binary,explain, session);
        this.session = session;
    }

    @Override
    public void sendError(Throwable e) {
        session.setLastMessage(e);
        sqlExecuterWriter.writeToMycatSession(MycatErrorResponse.INSTANCE);
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }


    @Override
    public void proxyUpdate(String defaultTargetName, String sql) {
        execute(ExplainDetail.create(UPDATE,Objects.requireNonNull(defaultTargetName), sql, null));
    }

    @Override
    public void tryBroadcastShow(String statement) {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        List<String> infos = new ArrayList<>();
        List<String> keySet = new ArrayList<>();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        keySet.add(metadataManager.getPrototype());
        for (String datasourceName : keySet) {
            try (DefaultConnection connection = connectionManager.getConnection(datasourceName)) {
                RowBaseIterator rowBaseIterator = connection.executeQuery(statement);
                this.sendResultSet(RowIterable.create(rowBaseIterator));
                return;
            } catch (Throwable e) {
                infos.add("数据源:" + datasourceName + " : " + e + "");
            }
        }
        MycatException mycatException = new MycatException("物理分片不存在能够正确处理:\n" + statement + " \n" + String.join(",\n", infos));
        LOGGER.error("", mycatException);
        this.sendError(mycatException);
    }


    @Override
    public void sendError(String errorMessage, int errorCode) {
        session.setLastMessage(errorMessage);
        session.setLastErrorCode(errorCode);
        sqlExecuterWriter.writeToMycatSession(MycatErrorResponse.INSTANCE);
    }

    @Override
    public void sendResultSet(RowIterable  rowIterable) {
        sqlExecuterWriter.writeToMycatSession(rowIterable);
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
                    proxyUpdate(session.getMySQLSession().getDatasourceName(), "ROLLBACK");
                    LOGGER.debug("session id:{} action: rollback from binding session", session.sessionId());
                    return;
                } else {
                    sendOk();
                    LOGGER.debug("session id:{} action: rollback from unbinding session", session.sessionId());
                    return;
                }
            case JDBC_TRANSACTION_TYPE: {
                transactionSession.rollback();
                LOGGER.debug("session id:{} action: rollback from xa", session.sessionId());
                sendOk();
                return;
            }
        }
    }

    @Override
    public void begin() {
        MycatDataContext dataContext = session.getDataContext();
        TransactionType transactionType = dataContext.transactionType();
        TransactionSession transactionSession = dataContext.getTransactionSession();
        switch (transactionType) {
            case PROXY_TRANSACTION_TYPE: {
                transactionSession.begin();
                LOGGER.debug("session id:{} action:{}", session.sessionId(), "begin exe success");
                sendOk();
                return;
            }
            case JDBC_TRANSACTION_TYPE: {
                transactionSession.begin();
                LOGGER.debug("session id:{} action: begin from xa", session.sessionId());
                sendOk();
                return;
            }
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
                    sendOk();
                    return;
                } else {
                    proxyUpdate(session.getMySQLSession().getDatasourceName(), "COMMIT");
                    LOGGER.debug("session id:{} action: commit from binding session", session.sessionId());
                    return;
                }
            case JDBC_TRANSACTION_TYPE: {
                transactionSession.commit();
                LOGGER.debug("session id:{} action: commit from xa", session.sessionId());
                sendOk();
            }
        }
    }

    @Override
    public void execute(ExplainDetail detail) {
        boolean master = session.isInTransaction() || !session.isAutocommit() || detail.getExecuteType().isMaster();
        ReplicaSelectorRuntime selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
        String datasource = selectorRuntime.getDatasourceNameByReplicaName(Objects.requireNonNull(detail.getTarget()), master, detail.getBalance());
        sqlExecuterWriter.writeToMycatSession(MycatProxyResponse.create(detail.getExecuteType(), datasource, detail.getSql()));
    }

    public void sendOk(){
        sendOk(0,0);
    }
    @Override
    public void sendOk(long lastInsertId, long affectedRow) {
        session.setLastInsertId(lastInsertId);
        session.setAffectedRows(affectedRow);
        sqlExecuterWriter.writeToMycatSession(MycatUpdateResponse.INSTANCE);
    }
}