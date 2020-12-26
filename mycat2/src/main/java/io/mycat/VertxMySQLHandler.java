package io.mycat;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.runtime.MycatDataContextImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class VertxMySQLHandler {
    private final VertxSessionImpl session;
    private MycatDataContextImpl mycatDataContext;
    private NetSocket socket;

    public VertxMySQLHandler(MycatDataContextImpl mycatDataContext, NetSocket socket) {
        this.mycatDataContext = mycatDataContext;
        this.socket = socket;
        this.session = new VertxSessionImpl(mycatDataContext, socket);
    }

    public void handle(int packetId, Buffer event, NetSocket socket) {
        session.setPacketId(packetId);
        ReadView readView = new ReadView(event);
        try {
            switch (readView.readByte()) {
                case MySQLCommandType.COM_SLEEP: {
                    handleSleep(this.session);
                    break;
                }
                case MySQLCommandType.COM_QUIT: {
                    handleQuit(this.session);
                    break;
                }
                case MySQLCommandType.COM_QUERY: {
                    String sql = new String(readView.readEOFStringBytes(), StandardCharsets.UTF_8);
                    handleQuery(sql, this.session);
                    break;
                }
                case MySQLCommandType.COM_INIT_DB: {
                    String schema = readView.readEOFString();
                    handleInitDb(schema, this.session);
                    break;
                }
                case MySQLCommandType.COM_PING: {
                    handlePing(this.session);
                    break;
                }

                case MySQLCommandType.COM_FIELD_LIST: {
                    String table = readView.readNULString();
                    String field = readView.readEOFString();
                    handleFieldList(table, field, this.session);
                    break;
                }
                case MySQLCommandType.COM_SET_OPTION: {
                    boolean option = readView.readFixInt(2) == 1;
                    handleSetOption(option, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_PREPARE: {
                    byte[] bytes = readView.readEOFStringBytes();
                    handlePrepareStatement(bytes, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
                    long statementId = readView.readFixInt(4);
                    int paramId = (int) readView.readFixInt(2);
                    byte[] data = readView.readEOFStringBytes();
                    handlePrepareStatementLongdata(statementId, paramId, data, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_EXECUTE: {
                    MycatDataContext dataContext = this.session.getDataContext();
                    dataContext.getPrepareInfo();
                    try {
                        long statementId = readView.readFixInt(4);
                        byte flags = readView.readByte();
                        long iteration = readView.readFixInt(4);
                        assert iteration == 1;
                        int numParams = getNumParamsByStatementId(statementId, this.session);
                        byte[] nullMap = null;
                        if (numParams > 0) {
                            nullMap = readView.readBytes((numParams + 7) / 8);
                        }
                        int[] params = null;
                        BindValue[] values = null;
                        boolean newParameterBoundFlag = readView.readByte() == 1;
                        if (newParameterBoundFlag) {
                            params = new int[numParams];
                            for (int i = 0; i < numParams; i++) {
                                params[i] = (int) readView.readFixInt(2);
                            }
                            values = new BindValue[numParams];
                            for (int i = 0; i < numParams; i++) {
                                BindValue bv = new BindValue();
                                bv.type = params[i];
                                if ((nullMap[i / 8] & (1 << (i & 7))) != 0) {
                                    bv.isNull = true;
                                } else {
                                    byte[] longData = getLongData(statementId, i, this.session);
                                    if (longData == null) {
                                        BindValueUtil.read(readView, bv, StandardCharsets.UTF_8);
                                    } else {
                                        bv.value = longData;
                                    }
                                }
                                values[i] = bv;
                            }
                            saveBindValue(statementId, values, this.session);
                        } else {
                            values = getLastBindValue(statementId, this.session);
                        }
                        handlePrepareStatementExecute(statementId, flags, params, values,
                                this.session);
                        break;
                    } finally {

                    }
                }
                case MySQLCommandType.COM_STMT_CLOSE: {
                    long statementId = readView.readFixInt(4);
                    handlePrepareStatementClose(statementId, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_FETCH: {
                    long statementId = readView.readFixInt(4);
                    long row = readView.readFixInt(4);
                    handlePrepareStatementFetch(statementId, row, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_RESET: {
                    long statementId = readView.readFixInt(4);
                    handlePrepareStatementReset(statementId, this.session);
                    break;
                }
                case MySQLCommandType.COM_CREATE_DB: {
                    String schema = readView.readEOFString();
                    ;
                    handleCreateDb(schema, this.session);
                    break;
                }
                case MySQLCommandType.COM_DROP_DB: {
                    String schema = readView.readEOFString();
                    handleDropDb(schema, this.session);
                    break;
                }
                case MySQLCommandType.COM_REFRESH: {
                    byte subCommand = readView.readByte();
                    handleRefresh(subCommand, this.session);
                    break;
                }
                case MySQLCommandType.COM_SHUTDOWN: {
                    try {
                        if (!readView.readFinished()) {
                            byte shutdownType = readView.readByte();
                            handleShutdown(shutdownType, this.session);
                        } else {
                            handleShutdown(0, this.session);
                        }
                    } finally {
                    }
                    break;
                }
                case MySQLCommandType.COM_STATISTICS: {
                    handleStatistics(this.session);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_INFO: {
                    handleProcessInfo(this.session);
                    break;
                }
                case MySQLCommandType.COM_CONNECT: {
                    handleConnect(this.session);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_KILL: {
                    long connectionId = readView.readFixInt(4);
                    handleProcessKill(connectionId, this.session);
                    break;
                }
                case MySQLCommandType.COM_DEBUG: {
                    handleDebug(this.session);
                    break;
                }
                case MySQLCommandType.COM_TIME: {
                    handleTime(this.session);

                    break;
                }
                case MySQLCommandType.COM_DELAYED_INSERT: {
                    handleDelayedInsert(this.session);
                    break;
                }
                case MySQLCommandType.COM_CHANGE_USER: {
                    String userName = readView.readNULString();
                    String authResponse = null;
                    String schemaName = null;
                    Integer characterSet = null;
                    String authPluginName = null;
                    HashMap<String, String> clientConnectAttrs = new HashMap<>();
                    int capabilities = this.session.getCapabilities();
                    if (MySQLServerCapabilityFlags.isCanDo41Anthentication(capabilities)) {
                        byte len = readView.readByte();
                        authResponse = readView.readFixString(len);
                    } else {
                        authResponse = readView.readNULString();
                    }
                    schemaName = readView.readNULString();
                    if (!readView.readFinished()) {
                        characterSet = (int) readView.readFixInt(2);
                        if (MySQLServerCapabilityFlags.isPluginAuth(capabilities)) {
                            authPluginName = readView.readNULString();
                        }
                        if (MySQLServerCapabilityFlags.isConnectAttrs(capabilities)) {
                            long kvAllLength = readView.readLenencInt();
                            if (kvAllLength != 0) {
                                clientConnectAttrs = new HashMap<>();
                            }
                            int count = 0;
                            while (count < kvAllLength) {
                                String k = readView.readLenencString();
                                String v = readView.readLenencString();
                                count += k.length();
                                count += v.length();
                                count += calcLenencLength(k.length());
                                count += calcLenencLength(v.length());
                                clientConnectAttrs.put(k, v);
                            }
                        }
                    }
                    handleChangeUser(userName, authResponse, schemaName, characterSet, authPluginName,
                            clientConnectAttrs, this.session);
                    break;
                }
                case MySQLCommandType.COM_RESET_CONNECTION: {
                    handleResetConnection(this.session);
                    break;
                }
                case MySQLCommandType.COM_DAEMON: {
                    handleDaemon(this.session);
                    break;
                }
                default: {
                    assert false;
                }
            }
        } catch (Throwable throwable) {
            this.session.writeErrorEndPacketBySyncInProcessError(0);
        }
    }

    private void saveBindValue(long statementId, BindValue[] values, VertxSession vertxSession) {

    }

    private BindValue[] getLastBindValue(long statementId, VertxSession vertxSession) {
        return new BindValue[0];
    }

    private void handlePrepareStatementExecute(long statementId, byte flags, int[] params, BindValue[] values, VertxSession vertxSession) {

    }

    private byte[] getLongData(long statementId, int i, VertxSession vertxSession) {
        return new byte[0];
    }

    private int getNumParamsByStatementId(long statementId, VertxSession vertxSession) {
        return 0;
    }

    private void handlePrepareStatementReset(long statementId, VertxSession vertxSession) {

    }

    private void handlePrepareStatementFetch(long statementId, long row, VertxSession vertxSession) {

    }

    private void handlePrepareStatementClose(long statementId, VertxSession vertxSession) {


    }

    private void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, VertxSession vertxSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.appendLongData(paramId, data);
        }
    }

    private void handlePrepareStatement(byte[] bytes, VertxSession vertxSession) {

    }


    public void handleQuery(String sql, VertxSession session) throws Exception {
        VertxResponse vertxResponse = new VertxJdbcResponseImpl(session, 1,false);
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (sqlStatement instanceof SQLSelectStatement){
            vertxResponse.proxySelectToPrototype(sql);
        }else {
            vertxResponse.sendOk(0, 0);
        }
    }

    private void prepare(Future<SqlConnection> connection) {
        connection.onSuccess(new Handler<SqlConnection>() {
            @Override
            public void handle(SqlConnection sqlConnection) {
                Future<PreparedStatement> prepare = sqlConnection.prepare(
                        "select * from db1/");
                prepare.onSuccess(new Handler<PreparedStatement>() {
                    @Override
                    public void handle(PreparedStatement preparedStatement) {
                        RowStream<Row> stream = preparedStatement.createStream(5);
                        stream.handler(new Handler<Row>() {
                            @Override
                            public void handle(Row row) {
                                System.out.println(row);
                            }
                        });
                    }
                });
            }
        });
    }

    private void normal2(String sql, Response response, List<Object> params, Future<SqlConnection> connection) {
        Handler<Throwable> exceptionhandler = new Handler<Throwable>() {
            @Override
            public void handle(Throwable throwable) {
                System.out.println();
            }
        };
        connection.onFailure(exceptionhandler);
        Future<SqlConnection> sqlConnectionFuture = connection.onSuccess(sqlConnection -> sqlConnection.prepare(sql));
        sqlConnectionFuture.onFailure(exceptionhandler);
        sqlConnectionFuture.onSuccess(preparedStatement -> {
            PreparedQuery<RowSet<Row>> rowSetPreparedQuery = preparedStatement.preparedQuery(sql);
            Future<SqlResult<Object>> execute = rowSetPreparedQuery.collecting(new Collector<Row, Object, Object>() {
                @Override
                public Supplier<Object> supplier() {
                    return new Supplier<Object>() {
                        @Override
                        public Object get() {
                            return null;
                        }
                    };

                }

                @Override
                public BiConsumer<Object, Row> accumulator() {
                    return new BiConsumer<Object, Row>() {
                        @Override
                        public void accept(Object o, Row row) {
                            System.out.println();
                        }
                    };
                }

                @Override
                public BinaryOperator<Object> combiner() {
                    return new BinaryOperator<Object>() {
                        @Override
                        public Object apply(Object o, Object o2) {
                            return null;
                        }
                    };
                }

                @Override
                public Function<Object, Object> finisher() {
                    return new Function<Object, Object>() {
                        @Override
                        public Object apply(Object o) {
                            return null;
                        }
                    };
                }

                @Override
                public Set<Characteristics> characteristics() {
                    return Collections.emptySet();
                }
            }).execute(Tuple.tuple());
            execute.onSuccess(new Handler<SqlResult<Object>>() {
                @Override
                public void handle(SqlResult<Object> objectSqlResult) {

                    System.out.println();
                }
            });
        });
    }

    private void normal(String sql, Response response, List<Object> params, Future<SqlConnection> connection) {
        Handler<Throwable> exceptionhandler = new Handler<Throwable>() {
            @Override
            public void handle(Throwable throwable) {
                System.out.println();
            }
        };
        connection.onFailure(exceptionhandler);
        Future<SqlConnection> sqlConnectionFuture = connection.onSuccess(sqlConnection -> sqlConnection.prepare(sql));
        sqlConnectionFuture.onFailure(exceptionhandler);
        sqlConnectionFuture.onSuccess(preparedStatement -> {
            PreparedQuery<RowSet<Row>> rowSetPreparedQuery = preparedStatement.preparedQuery(sql);
            Future<RowSet<Row>> rowSetFuture = rowSetPreparedQuery.execute(Tuple.tuple(params));
            rowSetFuture.onFailure(exceptionhandler);
            rowSetFuture.onSuccess(rows -> {
                RowSet<Row> result = rows;
                List<String> strings = result.columnsNames();
                boolean updatePacket = strings == null;
                long lastInsertId = result.property(MySQLClient.LAST_INSERTED_ID);
                int rowCount = result.rowCount();
                if (updatePacket) {
                    response.sendOk(rowCount, lastInsertId);
                } else {
                    VertxMycatRowMetaData vertxMycatRowMetaData = new VertxMycatRowMetaData(result.columnDescriptors());

                }
            });
        });
    }

    public void handleSleep(VertxSession session) {

    }

    public void handleQuit(VertxSession session) {
        session.close();
    }

    public void handleInitDb(String db, VertxSession session) {
        session.getDataContext().useShcema(db);
        session.writeOk(false);
    }

    public void handlePing(VertxSession session) {
        session.writeOk(false);
    }

    public void handleFieldList(String table, String filedWildcard, VertxSession session) {

    }

    public void handleSetOption(boolean on, VertxSession session) {

    }

    public void handleCreateDb(String schemaName, VertxSession session) {

    }

    public void handleDropDb(String schemaName, VertxSession session) {

    }

    public void handleRefresh(int subCommand, VertxSession session) {

    }

    public void handleShutdown(int shutdownType, VertxSession session) {

    }

    public void handleStatistics(VertxSession session) {

    }

    public void handleProcessInfo(VertxSession session) {

    }

    public void handleConnect(VertxSession session) {

    }

    public void handleProcessKill(long connectionId, VertxSession session) {

    }

    public void handleDebug(VertxSession session) {

    }

    public void handleTime(VertxSession session) {

    }

    public void handleChangeUser(String userName, String authResponse, String schemaName,
                                 int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                 VertxSession session) {

    }

    public void handleDelayedInsert(VertxSession session) {

    }

    public void handleResetConnection(VertxSession session) {

    }

    public void handleDaemon(VertxSession session) {

    }
}
