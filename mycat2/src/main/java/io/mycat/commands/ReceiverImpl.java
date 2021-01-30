package io.mycat.commands;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.resultset.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.proxy.session.MycatSession;
import io.vertx.core.impl.future.PromiseInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.mycat.ExecuteType.QUERY;
import static io.mycat.ExecuteType.UPDATE;


public class ReceiverImpl implements Response {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiverImpl.class);

    protected final MycatSession session;
    protected final SQLExecuterWriter sqlExecuterWriter;
    protected final MycatDataContext dataContext;

    public ReceiverImpl(MycatSession session, int stmtSize, boolean binary) {
        this.sqlExecuterWriter = new SQLExecuterWriter(stmtSize, binary, session, this);
        this.session = session;
        this.dataContext = this.session.getDataContext();
    }


    @Override
    public PromiseInternal<Void> sendError(Throwable e) {
        session.setLastMessage(e);
        return sqlExecuterWriter.writeToMycatSession(MycatErrorResponse.INSTANCE);
    }

    @Override
    public PromiseInternal<Void> proxySelect(String defaultTargetName, String statement) {
        return execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }


    @Override
    public PromiseInternal<Void> proxyUpdate(String defaultTargetName, String sql) {
        return execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), sql, null));
    }

    @Override
    public PromiseInternal<Void> proxySelectToPrototype(String statement) {
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        List<String> infos = new ArrayList<>();
        List<String> keySet = new ArrayList<>();
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        keySet.add(metadataManager.getPrototype());
        for (String datasourceName : keySet) {
            try (DefaultConnection connection = connectionManager.getConnection(datasourceName)) {
                RowBaseIterator rowBaseIterator = connection.executeQuery(statement);
                return sendResultSet(RowIterable.create(rowBaseIterator));
            } catch (Throwable e) {
                infos.add("数据源:" + datasourceName + " : " + e + "");
            }
        }
        MycatException mycatException = new MycatException("物理分片不存在能够正确处理:\n" + statement + " \n" + String.join(",\n", infos));
        LOGGER.error("", mycatException);
        return sendError(mycatException);
    }


    @Override
    public PromiseInternal<Void> sendError(String errorMessage, int errorCode) {
        session.setLastMessage(errorMessage);
        session.setLastErrorCode(errorCode);
        return sqlExecuterWriter.writeToMycatSession(MycatErrorResponse.INSTANCE);
    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowIterable rowIterable) {
        return sqlExecuterWriter.writeToMycatSession(rowIterable);
    }


    @Override
    public PromiseInternal<Void> rollback() {
        return sqlExecuterWriter.writeToMycatSession(MycatRollbackResponse.INSTANCE);
    }

    @Override
    public PromiseInternal<Void> begin() {
        return sqlExecuterWriter.writeToMycatSession(MycatBeginResponse.INSTANCE);
    }

    @Override
    public PromiseInternal<Void> commit() {
        return sqlExecuterWriter.writeToMycatSession(MycatCommitResponse.INSTANCE);
    }

    @Override
    public PromiseInternal<Void> execute(ExplainDetail detail) {
        boolean master = session.isInTransaction() || !session.isAutocommit() || detail.getExecuteType().isMaster();
        String datasource = session.getDataContext().resolveDatasourceTargetName(detail.getTarget(), master);
        return sqlExecuterWriter.writeToMycatSession(MycatProxyResponse.create(detail.getExecuteType(), datasource, detail.getSql()));
    }

    @Override
    public PromiseInternal<Void> sendOk() {
       return sqlExecuterWriter.writeToMycatSession(MycatUpdateResponse.INSTANCE);
    }

    @Override
    public PromiseInternal<Void> sendOk(long affectedRow, long lastInsertId) {
        session.setLastInsertId(lastInsertId);
        session.setAffectedRows(affectedRow);
        return sqlExecuterWriter.writeToMycatSession(MycatUpdateResponse.INSTANCE);
    }

    @Override
    public <T> T unWrapper(Class<T> clazz) {
        if (MycatSession.class == clazz) {
            return clazz.cast(session);
        }
        if (SQLExecuterWriter.class == clazz) {
            return clazz.cast(sqlExecuterWriter);
        }
        return null;
    }

    @Override
    public PromiseInternal<Void> sendResultSet(RowObservable rowIterable) {
        return sqlExecuterWriter.writeToMycatSession(rowIterable);
    }
}