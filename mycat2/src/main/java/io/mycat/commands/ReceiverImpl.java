package io.mycat.commands;

import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.mycat.beans.resultset.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.util.packet.*;
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
    public void sendError(Throwable e) {
        dataContext.getEmitter().onNext(new SendErrorWritePacket(null, e, 0) {
            @Override
            public void writeToSocket() {
                session.setLastMessage(e);
                sqlExecuterWriter.writeToMycatSession(MycatErrorResponse.INSTANCE);
            }
        });
    }

    @Override
    public void proxySelect(String defaultTargetName, String statement) {
        execute(ExplainDetail.create(QUERY, defaultTargetName, statement, null));
    }


    @Override
    public void proxyUpdate(String defaultTargetName, String sql) {
        execute(ExplainDetail.create(UPDATE, Objects.requireNonNull(defaultTargetName), sql, null));
    }

    @Override
    public void proxySelectToPrototype(String statement) {
        dataContext.getEmitter().onNext(new SendResultSetWritePacket(){

            @Override
            public void writeToSocket() {
                JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                List<String> infos = new ArrayList<>();
                List<String> keySet = new ArrayList<>();
                MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
                keySet.add(metadataManager.getPrototype());
                for (String datasourceName : keySet) {
                    try (DefaultConnection connection = connectionManager.getConnection(datasourceName)) {
                        RowBaseIterator rowBaseIterator = connection.executeQuery(statement);
                       sendResultSet(RowIterable.create(rowBaseIterator));
                        return;
                    } catch (Throwable e) {
                        infos.add("数据源:" + datasourceName + " : " + e + "");
                    }
                }
                MycatException mycatException = new MycatException("物理分片不存在能够正确处理:\n" + statement + " \n" + String.join(",\n", infos));
                LOGGER.error("", mycatException);
                sendError(mycatException);
            }
        });
    }


    @Override
    public void sendError(String errorMessage, int errorCode) {
        dataContext.getEmitter().onNext(new SendErrorWritePacket(errorMessage,null,errorCode){

            @Override
            public void writeToSocket() {
                session.setLastMessage(errorMessage);
                session.setLastErrorCode(errorCode);
                sqlExecuterWriter.writeToMycatSession(MycatErrorResponse.INSTANCE);
            }
        });
    }

    @Override
    public void sendResultSet(RowIterable rowIterable) {
        dataContext.getEmitter().onNext(new SendResultSetWritePacket(){

            @Override
            public void writeToSocket() {
                sqlExecuterWriter.writeToMycatSession(rowIterable);
            }
        });
    }


    @Override
    public void rollback() {
        dataContext.getEmitter().onNext(new RollbackWritePacket(){

            @Override
            public void writeToSocket() {
                sqlExecuterWriter.writeToMycatSession(MycatRollbackResponse.INSTANCE);
            }
        });

    }

    @Override
    public void begin() {
        dataContext.getEmitter().onNext(new BeginWritePacket(){

            @Override
            public void writeToSocket() {
                sqlExecuterWriter.writeToMycatSession(MycatBeginResponse.INSTANCE);
            }
        });
    }

    @Override
    public void commit() {
        dataContext.getEmitter().onNext(new CommitWritePacket(){

            @Override
            public void writeToSocket() {
                sqlExecuterWriter.writeToMycatSession(MycatCommitResponse.INSTANCE);
            }
        });

    }

    @Override
    public void execute(ExplainDetail detail) {
        dataContext.getEmitter().onNext(new ExplainWritePacket() {

            @Override
            public void writeToSocket() {
                boolean master = session.isInTransaction() || !session.isAutocommit() || detail.getExecuteType().isMaster();
                String datasource = session.getDataContext().resolveDatasourceTargetName(detail.getTarget(), master);
                sqlExecuterWriter.writeToMycatSession(MycatProxyResponse.create(detail.getExecuteType(), datasource, detail.getSql()));
            }
        });
    }

    public void sendOk() {
        dataContext.getEmitter().onNext(new SendOkWritePacket(){

            @Override
            public void writeToSocket() {
                sqlExecuterWriter.writeToMycatSession(MycatUpdateResponse.INSTANCE);
            }
        });
    }

    @Override
    public void sendOk(long affectedRow, long lastInsertId) {
        dataContext.getEmitter().onNext(new SendOkWritePacket(){

            @Override
            public void writeToSocket() {
                session.setLastInsertId(lastInsertId);
                session.setAffectedRows(affectedRow);
                sqlExecuterWriter.writeToMycatSession(MycatUpdateResponse.INSTANCE);
            }
        });
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
    public void sendResultSet(RowObservable rowIterable) {
        dataContext.getEmitter().onNext(new SendResultSetWritePacket() {
            @Override
            public void writeToSocket() {
                sqlExecuterWriter.writeToMycatSession(rowIterable);
            }
        });
    }
}