package io.mycat.mycatmysql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.Process;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.DefaultPreparedOKPacket;
import io.mycat.commands.MycatdbCommand;
import io.mycat.commands.ReceiverImpl;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.util.VertxUtil;
import io.mycat.util.packet.AbstractWritePacket;
import io.mycat.vertx.ReadView;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.NetSocket;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class MycatVertxMySQLHandler {
    private MycatVertxMysqlSession session;
    private MycatDataContext mycatDataContext;
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatVertxMySQLHandler.class);

    public MycatVertxMySQLHandler(MycatVertxMysqlSession MycatMysqlSession) {
        this.mycatDataContext = MycatMysqlSession.getDataContext();
        this.session = MycatMysqlSession;
        NetSocket socket = this.session.getSocket();
        socket.exceptionHandler(event -> {
            mycatDataContext.setLastMessage(event);
            MycatMysqlSession.writeErrorEndPacketBySyncInProcessError();
        });
    }


    @AllArgsConstructor
    public static class PendingMessage {
        private final int packetId;
        private final Buffer event;
        private final NetSocket socket;
    }

    private final AtomicBoolean handleIng = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<PendingMessage> pendingMessages = new ConcurrentLinkedQueue<>();

    public Buffer copyIfDirectBuf(Buffer event) {
        if (event instanceof BufferImpl && ((BufferImpl) event).byteBuf().isDirect()) {
            Buffer buffer = Buffer.buffer(event.length());
            buffer.appendBuffer(event);
            return buffer;
        } else {
            return event;
        }
    }

    public void handle(int packetId, Buffer event, NetSocket socket) {
        if (handleIng.compareAndSet(false, true)) {
            try {
                Process process = Process.getCurrentProcess();
                handle0(packetId, event, socket,process);
                checkPendingMessages();
            } finally {
                Process.setCurrentProcess(null);
                handleIng.set(false);
                // check if handle set handleIng gap
                checkPendingMessages();
            }
        } else {
            pendingMessages.offer(new PendingMessage(packetId, copyIfDirectBuf(event), socket));
        }
    }

    private void checkPendingMessages(){
        PendingMessage pendingMessage;
        while ((pendingMessage = pendingMessages.poll()) != null) {
            Process process = Process.getCurrentProcess();
            try {
                handle0(pendingMessage.packetId, pendingMessage.event, pendingMessage.socket, process);
            }finally {
                Process.setCurrentProcess(null);
            }
        }
    }

    public void handle0(int packetId, Buffer event, NetSocket socket,Process process) {
        session.setPacketId(packetId);
        ReadView readView = new ReadView(event);
        Future<?> promise;
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
                    promise = handleQuery(sql, this.session);
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
                    promise = handlePrepareStatement(bytes, this.session);
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

                    boolean newParameterBoundFlag =!readView.readFinished()&& readView.readByte() == 1;
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
                    promise = VertxUtil.newFailPromise(new MycatException(MycatErrorCode.ERR_NOT_SUPPORT,"无法识别的MYSQL数据包"));
                    assert false;
                }
            }
            promise.onComplete(o->{
                process.exit();
                if(o.failed()){
                    mycatDataContext.setLastMessage(o.cause());
                    this.session.writeErrorEndPacketBySyncInProcessError(0);
                }
                checkPendingMessages();
            });
        } catch (Throwable throwable) {
            process.exit();
            mycatDataContext.setLastMessage(throwable);
            this.session.writeErrorEndPacketBySyncInProcessError(0);
        }
    }

    private Disposable subscribe(Observable<AbstractWritePacket> observable){
        Disposable disposable = observable.subscribe(
        // 收到数据包
        new Consumer<AbstractWritePacket>() {
            @Override
            public void accept(AbstractWritePacket packet) throws Throwable {
                packet.run();
            }
        }, new Consumer<Throwable>() {
        // 异常
            @Override
            public void accept(Throwable throwable) throws Throwable {

            }
        }, new Action() {
        // 完毕
            @Override
            public void run() throws Throwable {
                // check if handle set handleIng gap
                checkPendingMessages();
            }
        });
        return disposable;
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

    private  Future<Void>  handlePrepareStatementExecute(long statementId, byte flags, int[] params, BindValue[] values, MycatVertxMysqlSession MycatMysqlSession) throws Exception {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        SQLStatement statement = preparedStatement.getSQLStatementByBindValue(values);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("preparestatement:{}", statement);
        }
        Response receiver = new ReceiverImpl(session, 1,true);
        return MycatdbCommand.execute(dataContext, receiver, statement);
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

    private  PromiseInternal<Void>  handlePrepareStatementReset(long statementId, MycatVertxMysqlSession MycatMysqlSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.resetLongData();
        }
        return session.writeOkEndPacket();
    }

    private  PromiseInternal<Void>  handlePrepareStatementFetch(long statementId, long row, MycatVertxMysqlSession MycatMysqlSession) {
        return MycatMysqlSession.writeErrorEndPacketBySyncInProcessError();
    }

    private PromiseInternal<Void>  handlePrepareStatementClose(long statementId, MycatVertxMysqlSession MycatMysqlSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        longPreparedStatementMap.remove(statementId);
        return VertxUtil.newSuccessPromise();
    }

    private PromiseInternal<Void> handlePrepareStatementLongdata(long statementId, int paramId, byte[] data, MycatVertxMysqlSession MycatMysqlSession) {
        MycatDataContext dataContext = session.getDataContext();
        Map<Long, io.mycat.PreparedStatement> longPreparedStatementMap = dataContext.getPrepareInfo();
        io.mycat.PreparedStatement preparedStatement = longPreparedStatementMap.get(statementId);
        if (preparedStatement != null) {
            preparedStatement.appendLongData(paramId, data);
        }
        return VertxUtil.newSuccessPromise();
    }

    private PromiseInternal<Void> handlePrepareStatement(byte[] bytes, MycatVertxMysqlSession MycatMysqlSession) {
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
            return session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), true);
        }
        session.writeBytes(MySQLPacketUtil.generatePrepareOk(info), false);
        if (info.getPrepareOkParametersCount() > 0 && info.getPrepareOkColumnsCount() == 0) {
            for (int i = 0; i < info.getPrepareOkParametersCount(); i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            if (deprecateEOF) {
                return session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params,
                        info.getPrepareOkParametersCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params,
                        info.getPrepareOkParametersCount()), false);
                return session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
        } else if (info.getPrepareOkParametersCount() == 0 && info.getPrepareOkColumnsCount() > 0) {
            for (int i = 0; i < info.getPrepareOkColumnsCount(); i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i), false);
            }
            if (deprecateEOF) {
                return session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), false);
                return session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
        } else {
            for (int i = 0; i < info.getPrepareOkParametersCount(); i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(params, i), false);
            }
            session.writeColumnEndPacket(false);
            for (int i = 0; i < info.getPrepareOkColumnsCount(); i++) {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields, i), false);
            }
            if (deprecateEOF) {
                return session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), true);
            } else {
                session.writeBytes(MySQLPacketUtil.generateColumnDefPayload(fields,
                        info.getPrepareOkColumnsCount()), false);
                return session.writeBytes(MySQLPacketUtil.generateEof(session.getWarningCount(),
                        session.getServerStatusValue()), true);
            }
        }
    }


    public Future<Void> handleQuery(String sql, MycatVertxMysqlSession session) throws Exception {
        return MycatdbCommand.INSTANCE.executeQuery(sql, mycatDataContext, (size) ->
                new ReceiverImpl(session, size, false));
    }

    public PromiseInternal<Void>  handleSleep(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void> handleQuit(MycatVertxMysqlSession session) {
       return session.close();
    }

    public PromiseInternal<Void>  handleInitDb(String db, MycatVertxMysqlSession session) {
        session.getDataContext().useShcema(db);
        return session.writeOk(false);
    }

    public PromiseInternal<Void>  handlePing(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleFieldList(String table, String filedWildcard, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleSetOption(boolean on, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleCreateDb(String schemaName, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleDropDb(String schemaName, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleRefresh(int subCommand, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleShutdown(int shutdownType, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleStatistics(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleProcessInfo(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleConnect(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleProcessKill(long connectionId, MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleDebug(MycatVertxMysqlSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    public PromiseInternal<Void>  handleTime(MycatVertxMysqlSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    public PromiseInternal<Void>  handleChangeUser(String userName, String authResponse, String schemaName,
                                 int charsetSet, String authPlugin, Map<String, String> clientConnectAttrs,
                                 MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleDelayedInsert(MycatVertxMysqlSession session) {
        return session.writeErrorEndPacketBySyncInProcessError();
    }

    public PromiseInternal<Void>  handleResetConnection(MycatVertxMysqlSession session) {
        session.resetSession();
        return session.writeOkEndPacket();
    }

    public PromiseInternal<Void>  handleDaemon(MycatVertxMysqlSession session) {
        return session.writeOkEndPacket();
    }
}
