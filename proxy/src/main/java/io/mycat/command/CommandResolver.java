/**
 * Copyright (C) <2021>  <chenjunwen>
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
package io.mycat.command;

import io.mycat.*;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;

import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class CommandResolver {

    public static void handle(MycatSession mycat, MySQLPacket curPacket,
                              CommandDispatcher commandHandler) {
        MycatMonitor.onCommandStart(mycat);

        Future<Void> endFuture = null;
        try {
            boolean isEmptyPayload = curPacket.readFinished();
            if (isEmptyPayload) {
                MycatMonitor.onLoadDataLocalInFileEmptyPacketStart(mycat);
                endFuture = commandHandler.handleContentOfFilenameEmptyOk(mycat);
                mycat.resetCurrentProxyPayload();
                MycatMonitor.onLoadDataLocalInFileEmptyPacketEnd(mycat);
                return;
            } else if (mycat.shouldHandleContentOfFilename()) {
                MycatMonitor.onLoadDataLocalInFileContextStart(mycat);
                endFuture = commandHandler.handleContentOfFilename(curPacket.readEOFStringBytes(), mycat);
                mycat.resetCurrentProxyPayload();
                MycatMonitor.onLoadDataLocalInFileContextEnd(mycat);
                return;
            }
            byte head = curPacket.getByte(curPacket.packetReadStartIndex());
            switch (head) {
                case MySQLCommandType.COM_SLEEP: {
                    MycatMonitor.onSleepCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleSleep(mycat);
                    MycatMonitor.onSleepCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_QUIT: {
                    MycatMonitor.onQuitCommandStart(mycat);
                    curPacket.readByte();
                    endFuture = commandHandler.handleQuit(mycat);
                    MycatMonitor.onQuitCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_QUERY: {
                    MycatMonitor.onQueryCommandStart(mycat);
                    curPacket.readByte();
                    byte[] bytes = curPacket.readEOFStringBytes();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleQuery(bytes, mycat);
                    break;
                }
                case MySQLCommandType.COM_INIT_DB: {
                    MycatMonitor.onInitDbCommandStart(mycat);
                    curPacket.readByte();
                    String schema = curPacket.readEOFString();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleInitDb(schema, mycat);
                    MycatMonitor.onInitDbCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_PING: {
                    MycatMonitor.onPingCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handlePing(mycat);
                    MycatMonitor.onPingCommandEnd(mycat);
                    break;
                }

                case MySQLCommandType.COM_FIELD_LIST: {
                    MycatMonitor.onFieldListCommandStart(mycat);
                    curPacket.readByte();
                    String table = curPacket.readNULString();
                    String field = curPacket.readEOFString();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleFieldList(table, field, mycat);
                    MycatMonitor.onFieldListCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_SET_OPTION: {
                    MycatMonitor.onSetOptionCommandStart(mycat);
                    curPacket.readByte();
                    boolean option = curPacket.readFixInt(2) == 1;
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleSetOption(option, mycat);
                    MycatMonitor.onSetOptionCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_STMT_PREPARE: {
                    MycatMonitor.onPrepareCommandStart(mycat);
                    curPacket.readByte();
                    byte[] bytes = curPacket.readEOFStringBytes();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handlePrepareStatement(bytes, mycat);
                    MycatMonitor.onPrepareCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
                    MycatMonitor.onSendLongDataCommandStart(mycat);
                    curPacket.readByte();
                    long statementId = curPacket.readFixInt(4);
                    int paramId = (int) curPacket.readFixInt(2);
                    byte[] data = curPacket.readEOFStringBytes();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handlePrepareStatementLongdata(statementId, paramId, data, mycat);
                    MycatMonitor.onSendLongDataCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_STMT_EXECUTE: {
                    MycatMonitor.onExecuteCommandStart(mycat);
                    MycatDataContext dataContext = mycat.getDataContext();
                    dataContext.getPrepareInfo();
                    try {
                        byte[] rawPayload = curPacket.getEOFStringBytes(curPacket.packetReadStartIndex());
                        curPacket.readByte();
                        long statementId = curPacket.readFixInt(4);
                        byte flags = curPacket.readByte();
                        long iteration = curPacket.readFixInt(4);
                        assert iteration == 1;
                        int numParams = commandHandler.getNumParamsByStatementId(statementId, mycat);
                        byte[] nullMap = null;
                        if (numParams > 0) {
                            nullMap = curPacket.readBytes((numParams + 7) / 8);
                        }
                        int[] params = null;
                        BindValue[] values = null;
                        boolean newParameterBoundFlag =!curPacket.readFinished()&&curPacket.readByte() == 1;
                        if (newParameterBoundFlag) {
                            params = new int[numParams];
                            for (int i = 0; i < numParams; i++) {
                                params[i] = (int) curPacket.readFixInt(2);
                            }
                            values = new BindValue[numParams];
                            for (int i = 0; i < numParams; i++) {
                                BindValue bv = new BindValue();
                                bv.type = params[i];
                                if ((nullMap[i / 8] & (1 << (i & 7))) != 0) {
                                    bv.isNull = true;
                                } else {
                                    byte[] longData = commandHandler.getLongData(statementId, i, mycat);
                                    if (longData == null) {
                                        BindValueUtil.read(curPacket, bv, StandardCharsets.UTF_8);
                                        bv.isLongData = false;
                                    } else {
                                        bv.value = longData;
                                        bv.isLongData = true;
                                    }
                                }
                                values[i] = bv;
                            }
                            commandHandler.saveBindValue(statementId, values, mycat);
                        } else {
                            values = commandHandler.getLastBindValue(statementId, mycat);
                        }
                        mycat.resetCurrentProxyPayload();
                        endFuture = commandHandler
                                .handlePrepareStatementExecute(statementId, flags, params, values,
                                        mycat);
                        break;
                    } finally {
                        MycatMonitor.onExecuteCommandEnd(mycat);
                    }
                }
                case MySQLCommandType.COM_STMT_CLOSE: {
                    MycatMonitor.onCloseCommandStart(mycat);
                    curPacket.readByte();
                    long statementId = curPacket.readFixInt(4);
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handlePrepareStatementClose(statementId, mycat);
                    endFuture = endFuture.onComplete(event -> mycat.onHandlerFinishedClear());
                    MycatMonitor.onCloseCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_STMT_FETCH: {
                    MycatMonitor.onFetchCommandStart(mycat);
                    curPacket.readByte();
                    long statementId = curPacket.readFixInt(4);
                    long row = curPacket.readFixInt(4);
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handlePrepareStatementFetch(statementId, row, mycat);
                    MycatMonitor.onFetchCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_STMT_RESET: {
                    MycatMonitor.onResetCommandStart(mycat);
                    curPacket.readByte();
                    long statementId = curPacket.readFixInt(4);
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handlePrepareStatementReset(statementId, mycat);
                    MycatMonitor.onResetCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_CREATE_DB: {
                    MycatMonitor.onCreateDbCommandStart(mycat);
                    curPacket.readByte();
                    String schema = curPacket.readEOFString();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleCreateDb(schema, mycat);
                    MycatMonitor.onCreateDbCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_DROP_DB: {
                    MycatMonitor.onDropDbCommandStart(mycat);
                    curPacket.readByte();
                    String schema = curPacket.readEOFString();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleDropDb(schema, mycat);
                    MycatMonitor.onDropDbCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_REFRESH: {
                    MycatMonitor.onRefreshCommandStart(mycat);
                    curPacket.readByte();
                    byte subCommand = curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleRefresh(subCommand, mycat);
                    MycatMonitor.onRefreshCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_SHUTDOWN: {
                    MycatMonitor.onShutdownCommandStart(mycat);
                    curPacket.readByte();
                    try {
                        if (!curPacket.readFinished()) {
                            byte shutdownType = curPacket.readByte();
                            mycat.resetCurrentProxyPayload();
                            endFuture = commandHandler.handleShutdown(shutdownType, mycat);
                        } else {
                            mycat.resetCurrentProxyPayload();
                            endFuture = commandHandler.handleShutdown(0, mycat);
                        }
                    } finally {
                        MycatMonitor.onShutdownCommandEnd(mycat);
                    }
                    break;
                }
                case MySQLCommandType.COM_STATISTICS: {
                    MycatMonitor.onStatisticsCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleStatistics(mycat);
                    MycatMonitor.onStatisticsCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_INFO: {
                    MycatMonitor.onProcessInfoCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleProcessInfo(mycat);
                    MycatMonitor.onProcessInfoCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_CONNECT: {
                    MycatMonitor.onConnectCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleConnect(mycat);
                    MycatMonitor.onConnectCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_PROCESS_KILL: {
                    MycatMonitor.onProcessKillCommandStart(mycat);
                    curPacket.readByte();
                    long connectionId = curPacket.readFixInt(4);
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleProcessKill(connectionId, mycat);
                    MycatMonitor.onProcessKillCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_DEBUG: {
                    MycatMonitor.onDebugCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleDebug(mycat);
                    MycatMonitor.onDebugCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_TIME: {
                    MycatMonitor.onTimeCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleTime(mycat);
                    MycatMonitor.onTimeCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_DELAYED_INSERT: {
                    MycatMonitor.onDelayedInsertCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleDelayedInsert(mycat);
                    MycatMonitor.onDelayedInsertCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_CHANGE_USER: {
                    MycatMonitor.onChangeUserCommandStart(mycat);
                    curPacket.readByte();
                    try {
                        String userName = curPacket.readNULString();
                        String authResponse = null;
                        String schemaName = null;
                        Integer characterSet = null;
                        String authPluginName = null;
                        HashMap<String, String> clientConnectAttrs = new HashMap<>();
                        int capabilities = mycat.getCapabilities();
                        if (MySQLServerCapabilityFlags.isCanDo41Anthentication(capabilities)) {
                            byte len = curPacket.readByte();
                            authResponse = curPacket.readFixString(len);
                        } else {
                            authResponse = curPacket.readNULString();
                        }
                        schemaName = curPacket.readNULString();
                        if (!curPacket.readFinished()) {
                            characterSet = (int) curPacket.readFixInt(2);
                            if (MySQLServerCapabilityFlags.isPluginAuth(capabilities)) {
                                authPluginName = curPacket.readNULString();
                            }
                            if (MySQLServerCapabilityFlags.isConnectAttrs(capabilities)) {
                                long kvAllLength = curPacket.readLenencInt();
                                if (kvAllLength != 0) {
                                    clientConnectAttrs = new HashMap<>();
                                }
                                int count = 0;
                                while (count < kvAllLength) {
                                    String k = curPacket.readLenencString();
                                    String v = curPacket.readLenencString();
                                    count += k.length();
                                    count += v.length();
                                    count += calcLenencLength(k.length());
                                    count += calcLenencLength(v.length());
                                    clientConnectAttrs.put(k, v);
                                }
                            }
                        }
                        mycat.resetCurrentProxyPayload();
                        endFuture = commandHandler
                                .handleChangeUser(userName, authResponse, schemaName, characterSet, authPluginName,
                                        clientConnectAttrs, mycat);
                    } finally {
                        MycatMonitor.onChangeUserCommandEnd(mycat);
                    }
                    break;
                }
                case MySQLCommandType.COM_RESET_CONNECTION: {
                    MycatMonitor.onResetConnectionCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleResetConnection(mycat);
                    MycatMonitor.onResetConnectionCommandEnd(mycat);
                    break;
                }
                case MySQLCommandType.COM_DAEMON: {
                    MycatMonitor.onDaemonCommandStart(mycat);
                    curPacket.readByte();
                    mycat.resetCurrentProxyPayload();
                    endFuture = commandHandler.handleDaemon(mycat);
                    MycatMonitor.onDaemonCommandEnd(mycat);
                    break;
                }
                default: {
                    assert false;
                }
            }

        } finally {

            Objects.requireNonNull(endFuture).onComplete(event -> {
                Future<Void> future = Future.succeededFuture();
                if (event.failed()){
                    mycat.setLastMessage(event.cause());
                    future = mycat.writeErrorEndPacketBySyncInProcessError();
                }
                future.onComplete(event1 -> {
                    MycatMonitor.onCommandEnd(mycat);
                });
            });

        }
    }
}