package io.mycat.vertxmycat;

import io.mycat.MycatException;
import io.mycat.api.MySQLAPI;
import io.mycat.api.callback.MySQLAPIExceptionCallback;
import io.mycat.api.collector.ResultSetCollector;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.ext.MySQLAPIImpl;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.util.MycatRowMetaDataImpl;
import io.mycat.util.VertxUtil;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.impl.datatype.DataType;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import lombok.NonNull;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;

public class NativeMySQLConnection implements NewMycatConnection {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySqlConnectionImpl.class);
    final MySQLClientSession mySQLClientSession;
    ;

    public NativeMySQLConnection(MySQLClientSession mySQLClientSession) {
        this.mySQLClientSession = mySQLClientSession;
    }

    @Override
    public Future<RowSet> query(String sql, List<Object> params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        throw new UnsupportedOperationException();
//        if (mySQLClientSession.getCurNIOHandler() != null) {
//            throw new IllegalArgumentException();
//        }
//        String curSql = sql;
//        PromiseInternal<io.vertx.sqlclient.SqlResult<Void>> promise = VertxUtil.newPromise();
//        if (mySQLClientSession.getIOThread() == Thread.currentThread()) {
//            MySQLAPIImpl mySQLAPI = new MySQLAPIImpl(mySQLClientSession);
//            mySQLAPI.query(sql, new ResultSetCollector() {
//                @Override
//                public void onResultSetStart() {
//
//                }
//
//                @Override
//                public void onResultSetEnd() {
//
//                }
//
//                @Override
//                public void onRowStart() {
//
//                }
//
//                @Override
//                public void onRowEnd() {
//
//                }
//
//                @Override
//                public void addNull(int columnIndex) {
//
//                }
//
//                @Override
//                public void addString(int columnIndex, String value) {
//
//                }
//
//                @Override
//                public void addBlob(int columnIndex, byte[] value) {
//
//                }
//
//                @Override
//                public void addValue(int columnIndex, long value, boolean isNUll) {
//
//                }
//
//                @Override
//                public void addValue(int columnIndex, double value, boolean isNUll) {
//
//                }
//
//                @Override
//                public void addValue(int columnIndex, byte value, boolean isNUll) {
//
//                }
//
//                @Override
//                public void addDecimal(int columnIndex, BigDecimal value) {
//
//                }
//
//                @Override
//                public void collectColumnList(ColumnDefPacket[] packets) {
//                    ColumnDefinition[] columnDefinitions = new ColumnDefinition[packets.length];
//                    int index = 0;
//                    for (ColumnDefPacket packet : packets) {
//                        String catalog = new String(packet.getColumnCatalog());
//                        String schema = new String(packet.getColumnSchema());
//                        String table = new String(packet.getColumnTable());
//                        String orgTable = new String(packet.getColumnOrgTable());
//                        String name = new String(packet.getColumnName());
//                        String orgName = new String(packet.getColumnOrgName());
//                        int characterSet = packet.getColumnCharsetSet();
//                        long columnLength = packet.getColumnLength();
//                        DataType type = DataType.valueOf(packet.getColumnType() == 15 ? 253 : packet.getColumnType());
//                        int flags = packet.getColumnFlags();
//                        byte decimals = packet.getColumnDecimals();
//                        columnDefinitions[index++] = new ColumnDefinition(
//                                catalog,
//                                schema,
//                                table,
//                                orgTable,
//                                name,
//                                orgName,
//                                characterSet,
//                                columnLength,
//                                type,
//                                flags,
//                                decimals
//                        );
//                    }
//
//
//                    collector.onColumnDef(new MycatRowMetaDataImpl(columnDefinitions));
//                }
//
//                @Override
//                public void addDate(int columnIndex, Date date) {
//
//                }
//            },new MySQLAPIExceptionCallback(){
//
//                @Override
//                public void onException(Exception exception, @NonNull MySQLAPI mySQLAPI) {
//
//                }
//
//                @Override
//                public void onFinished(boolean monopolize, @NonNull MySQLAPI mySQLAPI) {
//
//                }
//
//                @Override
//                public void onErrorPacket(@NonNull ErrorPacket errorPacket, boolean monopolize, @NonNull MySQLAPI mySQLAPI) {
//
//                }
//            });
//            VertxMycatTextCollector<Object, Object> resultSetHandler = new VertxMycatTextCollector<Object, Object>((Collector) collectorArg);
//            if (LOGGER.isDebugEnabled()) {
//                LOGGER.debug("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql);
//            }
//            resultSetHandler.request(mySQLClientSession, MySQLCommandType.COM_QUERY, curSql.getBytes(),
//                    new ResultSetCallBack<MySQLClientSession>() {
//                        @Override
//                        public void onFinishedSendException(Exception exception, Object sender,
//                                                            Object attr) {
//                            MycatException mycatException = new MycatException(MySQLErrorCode.ER_UNKNOWN_ERROR, exception.getMessage());
//                            LOGGER.error("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql, mycatException);
//                            promise.tryFail(mycatException);
//                        }
//
//                        @Override
//                        public void onFinishedException(Exception exception, Object sender, Object attr) {
//                            MycatException mycatException = new MycatException(MySQLErrorCode.ER_UNKNOWN_ERROR, exception.getMessage());
//                            LOGGER.error("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql, mycatException);
//                            promise.tryFail(mycatException);
//                        }
//
//                        @Override
//                        public void onFinished(boolean monopolize, MySQLClientSession mysql,
//                                               Object sender, Object attr) {
//                            if (LOGGER.isDebugEnabled()) {
//                                LOGGER.debug("onFinished session id:{} sql:{}", mySQLClientSession.sessionId(), curSql);
//                            }
//                            MySqlResult<Void> mySqlResult = new MySqlResult<>(
//                                    resultSetHandler.getRowCount(),
//                                    resultSetHandler.getAffectedRows(),
//                                    resultSetHandler.getLastInsertId(), null,
//                                    Optional.ofNullable(resultSetHandler.getRowResultDecoder())
//                                            .map(i -> i.rowDesc)
//                                            .map(i -> i.columnDescriptor())
//                                            .orElse(Collections.emptyList())
//                            );
//                            promise.complete(mySqlResult);
//                        }
//
//                        @Override
//                        public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
//                                                  MySQLClientSession mysql, Object sender, Object attr) {
//                            MycatException mycatException = new MycatException(errorPacket.getErrorCode(), errorPacket.getErrorMessageString());
//                            LOGGER.error("onErrorPacket session id:{} sql:{}", mySQLClientSession.sessionId(), curSql, mycatException);
//                            promise.tryFail(mycatException);
//                        }
//                    });
//        } else {
//            mySQLClientSession.getIOThread().addNIOJob(new NIOJob() {
//                @Override
//                public void run(ReactorEnvThread reactor) throws Exception {
//                    if (LOGGER.isDebugEnabled()) {
//                        LOGGER.error("nio job session id:{} sql:{}", mySQLClientSession.sessionId(), curSql, new Throwable());
//                    }
//                    runTextQuery(curSql, mySQLClientSession, collectorArg).onComplete(promise);
//                }
//
//                @Override
//                public void stop(ReactorEnvThread reactor, Exception reason) {
//                    promise.tryFail(reason);
//                }
//
//                @Override
//                public String message() {
//                    return "proxy query text result set";
//                }
//            });
//        }
//        return promise;
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<List<Object>> call(String sql) {
        return Future.failedFuture(new UnsupportedOperationException());
    }

    @Override
    public Future<SqlResult> insert(String sql, List<Object> params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<SqlResult> update(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<SqlResult> update(String sql, List<Object> params) {
        return null;
    }

    @Override
    public Future<Void> close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abandonConnection() {

    }

    @Override
    public Future<Void> abandonQuery() {
        return null;
    }
}
