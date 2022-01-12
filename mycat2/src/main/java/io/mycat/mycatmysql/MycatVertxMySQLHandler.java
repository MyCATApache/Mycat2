package io.mycat.mycatmysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.Process;
import io.mycat.*;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.commands.MycatdbCommand;
import io.mycat.commands.ReceiverImpl;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.config.ServerConfig;
import io.mycat.prototypeserver.mysql.PrototypeService;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.ReadView;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class MycatVertxMySQLHandler {
    private MycatVertxMysqlSession session;
    private MycatDataContext mycatDataContext;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatVertxMySQLHandler.class);
    private Future<Void> sequenceFuture = Future.succeededFuture();

    public MycatVertxMySQLHandler(MycatVertxMysqlSession session) {
        this.mycatDataContext = session.getDataContext();
        this.session = session;
        NetSocket socket = this.session.getSocket();
        socket.exceptionHandler(event -> {
            synchronized (MycatVertxMySQLHandler.this) {
                mycatDataContext.setLastMessage(event);
                sequenceFuture = session.writeErrorEndPacketBySyncInProcessError();//丢弃之前的请求
            }
        });
    }

    public void handle(int packetId, Buffer event, NetSocket socket) {
        synchronized (MycatVertxMySQLHandler.this) {
            sequenceFuture = sequenceFuture.compose(unused -> handle0(packetId, event));
        }
    }

    public Future<Void> handle0(int packetId, Buffer event) {
        Process process = Process.getCurrentProcess();
        session.setPacketId(packetId);
        ReadView readView = new ReadView(event);
        Future<Void> promise;
        try {
            byte command = readView.readByte();
            process.setCommand(command);
            process.setContext(mycatDataContext);
            switch (command) {
                case MySQLCommandType.COM_SLEEP: {
                    promise = handleSleep(this.session);
                    break;
                }
                case MySQLCommandType.COM_QUIT: {
                    promise = handleQuit(this.session);
                    break;
                }
                case MySQLCommandType.COM_QUERY: {
                    String sql = new String(readView.readEOFStringBytes(), StandardCharsets.UTF_8);
                    process.setQuery(sql);
                    process.setState(Process.State.INIT);
                    IOExecutor vertx = MetaClusterCurrent.wrapper(IOExecutor.class);
                    promise = vertx.executeBlocking((Handler<Promise<Void>>) event1 -> handleQuery(sql, session).onComplete(event1));
                    break;
                }
                case MySQLCommandType.COM_INIT_DB: {
                    String schema = readView.readEOFString();
                    promise = handleInitDb(schema, this.session);
                    break;
                }
                case MySQLCommandType.COM_PING: {
                    promise = handlePing(this.session);
                    break;
                }

                case MySQLCommandType.COM_FIELD_LIST: {
                    String table = readView.readNULString();
                    String field = readView.readEOFString();
                    promise = handleFieldList(table, field, this.session);
                    break;
                }
                case MySQLCommandType.COM_SET_OPTION: {
                    boolean option = readView.readFixInt(2) == 1;
                    promise = handleSetOption(option, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_PREPARE: {
                    byte[] bytes = readView.readEOFStringBytes();
                    IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
                    promise = ioExecutor.executeBlocking(voidPromise -> {
                        try {
                            handlePrepareStatement(bytes, session).onComplete(voidPromise);
                        } catch (Throwable throwable) {
                            voidPromise.fail(throwable);
                        }
                    });
                    break;
                }
                case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
                    long statementId = readView.readFixInt(4);
                    int paramId = (int) readView.readFixInt(2);
                    byte[] data = readView.readEOFStringBytes();
                    promise = handlePrepareStatementLongdata(statementId, paramId, data, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_EXECUTE: {
                    MycatDataContext dataContext = this.session.getDataContext();
                    Map<Long, PreparedStatement> prepareInfo = dataContext.getPrepareInfo();
                    long statementId = readView.readFixInt(4);
                    byte flags = readView.readByte();
                    long iteration = readView.readFixInt(4);
                    assert iteration == 1;
                    int numParams = getNumParamsByStatementId(statementId, this.session);
                    byte[] nullMap = null;
                    if (numParams > 0) {
                        nullMap = readView.readBytes((numParams + 7) / 8);
                    }
                    int[] params = prepareInfo.get(statementId).getParametersType();
                    BindValue[] values = new BindValue[numParams];

                    boolean newParameterBoundFlag = !readView.readFinished() && readView.readByte() == 1;
                    if (newParameterBoundFlag) {
                        for (int i = 0; i < numParams; i++) {
                            params[i] = (int) readView.readFixInt(2);
                        }
                    }
                    for (int i = 0; i < numParams; i++) {
                        BindValue bv = new BindValue();
                        bv.type = params[i];
                        if ((nullMap[i / 8] & (1 << (i & 7))) != 0) {
                            bv.isNull = true;
                        } else {
                            byte[] longData = getLongData(statementId, i, this.session);
                            if (longData == null) {
                                ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
                                BindValueUtil.read(readView, bv, StandardCharsets.UTF_8, !serverConfig.isPstmtStringVal());
                                bv.isLongData = false;
                            } else {
                                bv.value = longData;
                                bv.isLongData = true;
                            }
                        }
                        values[i] = bv;
                    }
                    saveBindValue(statementId, values, this.session);
                    promise = handlePrepareStatementExecute(statementId, flags, params, values,
                            this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_CLOSE: {
                    long statementId = readView.readFixInt(4);
                    promise = handlePrepareStatementClose(statementId, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_FETCH: {
                    long statementId = readView.readFixInt(4);
                    long row = readView.readFixInt(4);
                    promise = handlePrepareStatementFetch(statementId, row, this.session);
                    break;
                }
                case MySQLCommandType.COM_STMT_RESET: {
                    long statementId = readView.readFixInt(4);
                    promise = handlePrepareStatementReset(statementId, this.session);
                    break;
                }
                case MySQLCommandType.COM_CREATE_DB: {
                    String schema = readView.readEOFString();
                    promise = handleCreateDb(schema, this.session);
                    break;
                }
                case MySQLCommandType.COM_DROP_DB: {
                    String schema = readView.readEOFString();
                    promise = handleDropDb(schema, this.session);
                    break;
                }
                case MySQLCommandType.COM_REFRESH: {
                    byte subCommand = readView.readByte();
                    promise = handleRefresh(subCommand, this.session);
                    break;
                }
                case MySQLCommandType.COM_SHUTDOWN: {
                    try {
                        if (!readView.readFinished()) {
                            byte shutdownType = readView.readByte();
                            promise = handleShutdown(shutdownType, this.session);
                        } else {
                            promise = handleShutdown(0, this.session);
                        }
                    } finally {
                    }
                    break;
                }
                case MySQLCommandType.COM_STATISTICS: {
                    promise = handleStatistics(this.session);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_INFO: {
                    promise = handleProcessInfo(this.session);
                    break;
                }
                case MySQLCommandType.COM_CONNECT: {
                    promise = handleConnect(this.session);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_KILL: {
                    long connectionId = readView.readFixInt(4);
                    promise = handleProcessKill(connectionId, this.session);
                    break;
                }
                case MySQLCommandType.COM_DEBUG: {
                    promise = handleDebug(this.session);
                    break;
                }
                case MySQLCommandType.COM_TIME: {
                    promise = handleTime(this.session);

                    break;
                }
                case MySQLCommandType.COM_DELAYED_INSERT: {
                    promise = handleDelayedInsert(this.session);
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
                    promise = handleChangeUser(userName, authResponse, schemaName, characterSet, authPluginName,
                            clientConnectAttrs, this.session);
                    break;
                }
                case MySQLCommandType.COM_RESET_CONNECTION: {
                    promise = handleResetConnection(this.session);
                    break;
                }
                case MySQLCommandType.COM_DAEMON: {
                    promise = handleDaemon(this.session);
                    break;
                }
                default: {
                    promise = VertxUtil.newFailPromise(new MycatException(MycatErrorCode.ERR_NOT_SUPPORT, "无法识别的MYSQL数据包"));
                    assert false;
                }
            }
            return promise.recover(cause -> {
                int errorCode = 0;
                String message;
                String sqlState;
                if (cause instanceof SQLException) {
                    errorCode = ((SQLException) cause).getErrorCode();
                    message = ((SQLException) cause).getMessage();
                    sqlState = ((SQLException) cause).getSQLState();
                } else if (cause instanceof MycatException) {
                    errorCode = ((MycatException) cause).getErrorCode();
                    message = ((MycatException) cause).getMessage();
                    sqlState = "";
                } else {
                    message = cause.toString();
                }
                mycatDataContext.setLastMessage(message);
                return this.session.writeErrorEndPacketBySyncInProcessError(errorCode);
            });
        } catch (Throwable throwable) {
            mycatDataContext.setLastMessage(throwable);
            return this.session.writeErrorEndPacketBySyncInProcessError(0);
        }
    }

    private void saveBindValue(long statementId, BindValue[] values, MycatVertxMysqlSession MycatMysqlSession) {
        Map<Long, io.mycat.PreparedStatement> prepareInfo = mycatDataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return;
        }
        preparedStatement.setBindValues(values);
    }

    private BindValue[] getLastBindValue(long statementId, MycatVertxMysqlSession MycatMysqlSession) {
        Map<Long, io.mycat.PreparedStatement> prepareInfo = mycatDataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return null;
        }
        return preparedStatement.getBindValues();
    }

    private Future<Void> handlePrepareStatementExecute(long statementId, byte flags, int[] params, BindValue[] values, MycatVertxMysqlSession MycatMysqlSession) throws Exception {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        SQLStatement statement = preparedStatement.getSQLStatementByBindValue(values);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("preparestatement:{}", statement);
        }
        SQLStatement typeStatement = metadataManager.typeInferenceUpdate(statement, dataContext.getDefaultSchema());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("typeInferenceUpdate:{}", typeStatement);
        }
        Response receiver = new ReceiverImpl(session, 1, true);
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(event -> MycatdbCommand.execute(dataContext, receiver, typeStatement).onComplete(event));
    }

    private byte[] getLongData(long statementId, int i, MycatVertxMysqlSession MycatMysqlSession) {
        Map<Long, io.mycat.PreparedStatement> preparedStatementMap = mycatDataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = preparedStatementMap.get(statementId);
        ByteArrayOutputStream longData = preparedStatement.getLongData(i);
        if (longData == null) {
            return null;
        }
        return longData.toByteArray();
    }

    private int getNumParamsByStatementId(long statementId, MycatVertxMysqlSession MycatMysqlSession) {
        Map<Long, io.mycat.PreparedStatement> preparedStatementMap = mycatDataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = Objects.requireNonNull(
                preparedStatementMap.get(statementId),
                () -> "preparedStatement:" + statementId + "  not exist"
        );
        return preparedStatement.getParametersNumber();
    }

    private Future<Void> handlePrepareStatementReset(long statementId, MycatVertxMysqlSession MycatMysqlSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.resetLongData();
        }
        return session.writeOkEndPacket();
    }

    private Future<Void> handlePrepareStatementFetch(long statementId, long row, MycatVertxMysqlSession MycatMysqlSession) {
        return MycatMysqlSession.writeErrorEndPacketBySyncInProcessError();
    }

    private Future<Void> handlePrepareStatementClose(long statementId, MycatVertxMysqlSession MycatMysqlSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        longPreparedStatementMap.remove(statementId);
        return VertxUtil.newSuccessPromise();
    }

    private Future<Void> handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatVertxMysqlSession MycatMysqlSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.appendLongData(paramId, data);
        }
        return VertxUtil.newSuccessPromise();
    }

    private Future<Void> handlePrepareStatement(byte[] bytes, MycatVertxMysqlSession mysqlSession) {
        boolean deprecateEOF = mysqlSession.isDeprecateEOF();
        String sql = new String(bytes);
        /////////////////////////////////////////////////////
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("received pstmt sql:{}", sql);
        }
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        boolean allow = (sqlStatement instanceof SQLSelectStatement
                ||
                sqlStatement instanceof SQLInsertStatement
                ||
                sqlStatement instanceof SQLUpdateStatement
                ||
                sqlStatement instanceof SQLDeleteStatement
        );
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        MycatRowMetaData fields;
        if ((sqlStatement instanceof SQLSelectStatement)) {
            PrototypeService prototypeService = MetaClusterCurrent.wrapper(PrototypeService.class);
            Optional<MycatRowMetaData> mycatRowMetaDataForPrepareStatement = prototypeService.getMycatRowMetaDataForPrepareStatement(mysqlSession.getDataContext().getDefaultSchema(), sql);

            if (!mycatRowMetaDataForPrepareStatement.isPresent()) {
                return VertxUtil.castPromise(Future.failedFuture(new SQLException("This command is not supported in the prepared statement protocol yet", "HY000", 1295)));
            }
            fields = mycatRowMetaDataForPrepareStatement.get();
        } else {
            fields = ResultSetBuilder.create().build().getMetaData();
        }

        ResultSetBuilder paramsBuilder = ResultSetBuilder.create();

        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    JDBCType res = JDBCType.VARCHAR;
                    paramsBuilder.addColumnInfo("", res);
                }
                super.endVisit(x);
            }
        });

        MycatRowMetaData params = paramsBuilder.build().getMetaData();
        long stmtId = mycatDataContext.nextPrepareStatementId();
        Map<Long, io.mycat.PreparedStatement> statementMap = this.mycatDataContext.getPrepareInfo();

        PreparedStatement preparedStatement = new PreparedStatement(stmtId, sqlStatement, params.getColumnCount());
        for (int i = 0; i < params.getColumnCount(); i++) {
            preparedStatement.getParametersType()[i] = MysqlDefs.FIELD_TYPE_STRING;
        }

        statementMap.put(stmtId, preparedStatement);

        DefaultPreparedOKPacket info = new DefaultPreparedOKPacket(stmtId, fields.getColumnCount(), params.getColumnCount(), session.getWarningCount());

        Future<Void> writeEndFuture = Future.succeededFuture();
        if (info.getPrepareOkColumnsCount() == 0 && info.getPrepareOkParametersCount() == 0) {
            return session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), true);
        }
        session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), false);
        if (info.getPrepareOkParametersCount() > 0 && info.getPrepareOkColumnsCount() == 0) {
            for (int i = 0; i < info.getPrepareOkParametersCount(); i++) {
                writeEndFuture = session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i),
                        info.getPrepareOkParametersCount() - 1 == i && deprecateEOF);
            }
            if (deprecateEOF) {
                return writeEndFuture;
            } else {
                return session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
        } else if (info.getPrepareOkParametersCount() == 0 && info.getPrepareOkColumnsCount() > 0) {
            for (int i = 0; i < info.getPrepareOkColumnsCount(); i++) {
                writeEndFuture = session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i),
                        info.getPrepareOkColumnsCount() - 1 == i && deprecateEOF);
            }
            if (deprecateEOF) {
                return writeEndFuture;
            } else {
                return session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
        } else {
            for (int i = 0; i < info.getPrepareOkParametersCount(); i++) {
                writeEndFuture = session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            writeEndFuture = session.writeColumnEndPacket(false);
            for (int i = 0; i < info.getPrepareOkColumnsCount(); i++) {
                writeEndFuture = session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i),
                        info.getPrepareOkColumnsCount() - 1 == i && deprecateEOF);
            }
            if (deprecateEOF) {
                return writeEndFuture;
            } else {
                return session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
        }
    }


    public Future<Void> handleQuery(String sql, MycatVertxMysqlSession session) {
        return MycatdbCommand.INSTANCE.executeQuery(sql, mycatDataContext, (size) ->
                new ReceiverImpl(session, size, false));
    }

    public Future<Void> handleSleep(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleQuit(MycatVertxMysqlSession session) {
        return session.close();
    }

    public Future<Void> handleInitDb(String db, MycatVertxMysqlSession session) {
        session.getDataContext().useShcema(db);
        return session.writeOk(false);
    }

    public Future<Void> handlePing(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleFieldList(String table, String filedWildcard, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleSetOption(boolean on, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleCreateDb(String schemaName, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleDropDb(String schemaName, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleRefresh(int subCommand, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleShutdown(int shutdownType, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleStatistics(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleProcessInfo(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleConnect(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleProcessKill(long connectionId, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleDebug(MycatVertxMysqlSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    public Future<Void> handleTime(MycatVertxMysqlSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    public Future<Void> handleChangeUser(String userName, String authResponse, String schemaName,
                                         int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                         MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public Future<Void> handleDelayedInsert(MycatVertxMysqlSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    public Future<Void> handleResetConnection(MycatVertxMysqlSession session) {
        session.resetSession();
        return session.writeOkEndPacket();
    }

    public Future<Void> handleDaemon(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }
}
