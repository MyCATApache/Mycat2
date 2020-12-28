package io.mycat;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.commands.MycatdbCommand;
import io.mycat.commands.ReceiverImpl;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.session.TranscationSwitch;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.sqlrecorder.SqlRecord;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxMySQLHandler.class);

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
        Map<Long, io.mycat.PreparedStatement> prepareInfo = mycatDataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return;
        }
        preparedStatement.setBindValues(values);
    }

    private BindValue[] getLastBindValue(long statementId, VertxSession vertxSession) {
        Map<Long, io.mycat.PreparedStatement> prepareInfo = mycatDataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = prepareInfo.get(statementId);
        if (preparedStatement == null) {
            return null;
        }
        return preparedStatement.getBindValues();
    }

    private void handlePrepareStatementExecute(long statementId, byte flags, int[] params, BindValue[] values, VertxSession vertxSession) throws Exception {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        SQLStatement statement = preparedStatement.getSQLStatementByBindValue(values);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("=> {}", statement);
        }
        Response receiver = new VertxJdbcResponseImpl(vertxSession, 1, true);
        MycatdbCommand.execute(dataContext, receiver, statement);
    }

    private byte[] getLongData(long statementId, int i, VertxSession vertxSession) {
        Map<Long, io.mycat.PreparedStatement> preparedStatementMap = mycatDataContext.getPreparedStatementMap();
        io.mycat.PreparedStatement preparedStatement = preparedStatementMap.get(statementId);
        ByteArrayOutputStream longData = preparedStatement.getLongData(i);
        if (longData == null) {
            return null;
        }
        return longData.toByteArray();
    }

    private int getNumParamsByStatementId(long statementId, VertxSession vertxSession) {
        Map<Long, io.mycat.PreparedStatement> preparedStatementMap = mycatDataContext.getPreparedStatementMap();
        io.mycat.PreparedStatement preparedStatement = Objects.requireNonNull(
                preparedStatementMap.get(statementId),
                () -> "preparedStatement:" + statementId + "  not exist"
        );
        return preparedStatement.getParametersNumber();
    }

    private void handlePrepareStatementReset(long statementId, VertxSession vertxSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.resetLongData();
        }
        session.writeOkEndPacket();
    }

    private void handlePrepareStatementFetch(long statementId, long row, VertxSession vertxSession) {
        vertxSession.writeErrorEndPacketBySyncInProcessError();
    }

    private void handlePrepareStatementClose(long statementId, VertxSession vertxSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        longPreparedStatementMap.remove(statementId);
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
        boolean deprecateEOF = session.isDeprecateEOF();
        String sql = new String(bytes);
        /////////////////////////////////////////////////////

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
        metadataManager.resolveMetadata(sqlStatement);
        ResultSetBuilder fieldsBuilder = ResultSetBuilder.create();
        MycatRowMetaData fields = fieldsBuilder.build().getMetaData();
        ResultSetBuilder paramsBuilder = ResultSetBuilder.create();

        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    SQLDataType sqlDataType = x.computeDataType();
                    JDBCType res = JDBCType.VARCHAR;
                    if (sqlDataType != null) {
                        res = JDBCType.valueOf(sqlDataType.jdbcType());
                    }
                    paramsBuilder.addColumnInfo("", res);
                }
                super.endVisit(x);
            }
        });

        MycatRowMetaData params = paramsBuilder.build().getMetaData();
        long stmtId = mycatDataContext.nextPrepareStatementId();
        Map<Long, io.mycat.PreparedStatement> statementMap = this.mycatDataContext.getPrepareInfo();
        statementMap.put(stmtId, new io.mycat.PreparedStatement(stmtId, sqlStatement, params.getColumnCount()));

        DefaultPreparedOKPacket info = new DefaultPreparedOKPacket(stmtId, fields.getColumnCount(), params.getColumnCount(), session.getWarningCount());

        if (info.getPrepareOkColumnsCount() == 0 && info.getPrepareOkParametersCount() == 0) {
            session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), true);
            return;
        }
        session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), false);
        if (info.getPrepareOkParametersCount() > 0 && info.getPrepareOkColumnsCount() == 0) {
            for (int i = 1; i <= info.getPrepareOkParametersCount() - 1; i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            if (deprecateEOF) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params,
                        info.getPrepareOkParametersCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params,
                        info.getPrepareOkParametersCount()), false);
                session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
            return;
        } else if (info.getPrepareOkParametersCount() == 0 && info.getPrepareOkColumnsCount() > 0) {
            for (int i = 1; i <= info.getPrepareOkColumnsCount() - 1; i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i), false);
            }
            if (deprecateEOF) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), false);
                session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
            return;
        } else {
            for (int i = 1; i <= info.getPrepareOkParametersCount(); i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            session.writeColumnEndPacket();
            for (int i = 1; i <= info.getPrepareOkColumnsCount() - 1; i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i), false);
            }
            if (deprecateEOF) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), false);
                session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
            return;
        }
    }


    public void handleQuery(String sql, VertxSession session) throws Exception {
        TranscationSwitch transcationSwitch = MetaClusterCurrent.wrapper(TranscationSwitch.class);
        transcationSwitch.ensureTranscation(mycatDataContext);
        MycatdbCommand.INSTANCE.executeQuery(sql,mycatDataContext,(size)->
                new VertxJdbcResponseImpl(session,size, false));
    }

    public void handleSleep(VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleQuit(VertxSession session) {
        session.close();
    }

    public void handleInitDb(String db, VertxSession session) {
        session.getDataContext().useShcema(db);
        session.writeOk(false);
    }

    public void handlePing(VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleFieldList(String table, String filedWildcard, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleSetOption(boolean on, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleCreateDb(String schemaName, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleDropDb(String schemaName, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleRefresh(int subCommand, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleShutdown(int shutdownType, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleStatistics(VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleProcessInfo(VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleConnect(VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleProcessKill(long connectionId, VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleDebug(VertxSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    public void handleTime(VertxSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    public void handleChangeUser(String userName, String authResponse, String schemaName,
                                 int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                 VertxSession session) {
        session.writeOkEndPacket();
    }

    public void handleDelayedInsert(VertxSession session) {
        session.writeErrorEndPacketBySyncInProcessError();
    }

    public void handleResetConnection(VertxSession session) {
        session.resetSession();
        session.writeOkEndPacket();
    }

    public void handleDaemon(VertxSession session) {
        session.writeOkEndPacket();
    }
}
