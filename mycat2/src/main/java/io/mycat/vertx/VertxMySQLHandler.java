/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.vertx;

import io.mycat.*;
import io.mycat.beans.mycat.MycatErrorCode;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.commands.DefaultCommandHandler;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.util.VertxUtil;
import io.mycat.Process;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.net.NetSocket;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class VertxMySQLHandler extends DefaultCommandHandler {
    private VertxSession session;
    private MycatDataContext mycatDataContext;
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxMySQLHandler.class);

    public VertxMySQLHandler(VertxSession vertxSession) {
        this.mycatDataContext = vertxSession.getDataContext();
        this.session = vertxSession;
        NetSocket socket = this.session.getSocket();
        socket.exceptionHandler(event -> {
            mycatDataContext.setLastMessage(event);
            vertxSession.writeErrorEndPacketBySyncInProcessError();
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
                handle0(packetId, event, socket);
                checkPendingMessages();
            } finally {
                handleIng.set(false);
                // check if handle set handleIng gap
                checkPendingMessages();
            }
        } else {
            pendingMessages.offer(new PendingMessage(packetId, copyIfDirectBuf(event), socket));
        }
    }

    private void checkPendingMessages() {
        PendingMessage pendingMessage;
        while ((pendingMessage = pendingMessages.poll()) != null) {
            handle0(pendingMessage.packetId, pendingMessage.event, pendingMessage.socket);
        }
    }

    public void handle0(int packetId, Buffer event, NetSocket socket) {
        session.setPacketId(packetId);
        ReadView readView = new ReadView(event);
        Process process = Process.getCurrentProcess();
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
                    byte[] queryBytes = readView.readEOFStringBytes();
                    process.setQuery(new String(queryBytes, StandardCharsets.UTF_8));
                    process.setState(Process.State.INIT);
                    promise = handleQuery(queryBytes, this.session);
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
                    boolean newParameterBoundFlag = !readView.readFinished()&&readView.readByte() == 1;
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
                    promise = VertxUtil.newFailPromise(new MycatException(MycatErrorCode.ERR_NOT_SUPPORT, "无法识别的MYSQL数据包"));
                    assert false;
                }
            }
            promise.onComplete(o -> {
                if (o.failed()) {
                    mycatDataContext.setLastMessage(o.cause());
                    this.session.writeErrorEndPacketBySyncInProcessError(0);
                }
                checkPendingMessages();
            });
        } catch (Throwable throwable) {
            mycatDataContext.setLastMessage(throwable);
            this.session.writeErrorEndPacketBySyncInProcessError(0);
        }
    }
}
