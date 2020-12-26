package io.mycat;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.commands.MycatdbCommand;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.desc.ColumnDescriptor;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static io.mycat.MysqlProxyServer.mySQLClientManager;
import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class VertxMySQLHandler {


    public void handle(Buffer event, NetSocket socket) {
        ReadView readView = new ReadView(event);
        VertxSession vertxSession = new VertxSessionImpl(socket);
        try {
            switch (readView.readByte()) {
                case MySQLCommandType.COM_SLEEP: {
                    handleSleep(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_QUIT: {
                    handleQuit(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_QUERY: {
                    String sql = new String(readView.readEOFStringBytes(), StandardCharsets.UTF_8);
                    handleQuery(sql, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_INIT_DB: {
                    String schema = readView.readEOFString();
                    handleInitDb(schema, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_PING: {
                    handlePing(vertxSession);
                    break;
                }

                case MySQLCommandType.COM_FIELD_LIST: {
                    String table = readView.readNULString();
                    String field = readView.readEOFString();
                    handleFieldList(table, field, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_SET_OPTION: {
                    boolean option = readView.readFixInt(2) == 1;
                    handleSetOption(option, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_STMT_PREPARE: {
                    byte[] bytes = readView.readEOFStringBytes();
                    handlePrepareStatement(bytes, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
                    long statementId = readView.readFixInt(4);
                    int paramId = (int) readView.readFixInt(2);
                    byte[] data = readView.readEOFStringBytes();
                    handlePrepareStatementLongdata(statementId, paramId, data, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_STMT_EXECUTE: {
                    MycatDataContext dataContext = vertxSession.getDataContext();
                    dataContext.getPrepareInfo();
                    try {
                        long statementId = readView.readFixInt(4);
                        byte flags = readView.readByte();
                        long iteration = readView.readFixInt(4);
                        assert iteration == 1;
                        int numParams = getNumParamsByStatementId(statementId, vertxSession);
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
                                    byte[] longData = getLongData(statementId, i, vertxSession);
                                    if (longData == null) {
                                        BindValueUtil.read(readView, bv, StandardCharsets.UTF_8);
                                    } else {
                                        bv.value = longData;
                                    }
                                }
                                values[i] = bv;
                            }
                            saveBindValue(statementId, values, vertxSession);
                        } else {
                            values = getLastBindValue(statementId, vertxSession);
                        }
                        handlePrepareStatementExecute(statementId, flags, params, values,
                                vertxSession);
                        break;
                    } finally {

                    }
                }
                case MySQLCommandType.COM_STMT_CLOSE: {
                    long statementId = readView.readFixInt(4);
                    handlePrepareStatementClose(statementId, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_STMT_FETCH: {
                    long statementId = readView.readFixInt(4);
                    long row = readView.readFixInt(4);
                    handlePrepareStatementFetch(statementId, row, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_STMT_RESET: {
                    long statementId = readView.readFixInt(4);
                    handlePrepareStatementReset(statementId, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_CREATE_DB: {
                    String schema = readView.readEOFString();
                    ;
                    handleCreateDb(schema, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_DROP_DB: {
                    String schema = readView.readEOFString();
                    handleDropDb(schema, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_REFRESH: {
                    byte subCommand = readView.readByte();
                    handleRefresh(subCommand, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_SHUTDOWN: {
                    try {
                        if (!readView.readFinished()) {
                            byte shutdownType = readView.readByte();
                            handleShutdown(shutdownType, vertxSession);
                        } else {
                            handleShutdown(0, vertxSession);
                        }
                    } finally {
                    }
                    break;
                }
                case MySQLCommandType.COM_STATISTICS: {
                    handleStatistics(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_INFO: {
                    handleProcessInfo(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_CONNECT: {
                    handleConnect(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_KILL: {
                    long connectionId = readView.readFixInt(4);
                    handleProcessKill(connectionId, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_DEBUG: {
                    handleDebug(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_TIME: {
                    handleTime(vertxSession);

                    break;
                }
                case MySQLCommandType.COM_DELAYED_INSERT: {
                    handleDelayedInsert(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_CHANGE_USER: {
                    String userName = readView.readNULString();
                    String authResponse = null;
                    String schemaName = null;
                    Integer characterSet = null;
                    String authPluginName = null;
                    HashMap<String, String> clientConnectAttrs = new HashMap<>();
                    int capabilities = vertxSession.getCapabilities();
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
                            clientConnectAttrs, vertxSession);
                    break;
                }
                case MySQLCommandType.COM_RESET_CONNECTION: {
                    handleResetConnection(vertxSession);
                    break;
                }
                case MySQLCommandType.COM_DAEMON: {
                    handleDaemon(vertxSession);
                    break;
                }
                default: {
                    assert false;
                }
            }
        } catch (Throwable throwable) {
            vertxSession.writeError(throwable);
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

    }

    private void handlePrepareStatement(byte[] bytes, VertxSession vertxSession) {

    }


    public void handleQuery(String sql, VertxSession session) throws Exception {

        MycatDataContext dataContext = session.getDataContext();
        LinkedList<SQLStatement> statements =
                MycatdbCommand.INSTANCE.parse(sql);
        Response response = new VertxResponse(session, statements.size());
        String sqlText = sql;
        List<Object> params = new ArrayList<>();
        MySQLPool ds1 = mySQLClientManager.map.get("ds");
        Future<SqlConnection> connection = ds1.getConnection();
//        prepare(connection);
        normal("select * from db1.travelrecord", response, params, connection);

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
            PreparedQuery<RowSet<Row>> rowSetPreparedQuery =  preparedStatement.preparedQuery(sql);
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
                if (updatePacket){
                    response.sendOk(rowCount,lastInsertId);
                }else {
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
        session.sendOk();
    }

    public void handlePing(VertxSession session) {
        session.sendOk();
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
