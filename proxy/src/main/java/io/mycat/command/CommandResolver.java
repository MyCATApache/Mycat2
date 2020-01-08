package io.mycat.command;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;

import java.util.HashMap;

import static io.mycat.beans.mysql.packet.AuthPacket.calcLenencLength;

public class CommandResolver {

  public static void handle(MycatSession mycat, MySQLPacket curPacket,
      CommandDispatcher commandHandler) {
    MycatMonitor.onCommandStart(mycat);
    try {
      boolean isEmptyPayload = curPacket.readFinished();
      if (isEmptyPayload) {
        MycatMonitor.onLoadDataLocalInFileEmptyPacketStart(mycat);
        commandHandler.handleContentOfFilenameEmptyOk(mycat);
        mycat.resetCurrentProxyPayload();
        MycatMonitor.onLoadDataLocalInFileEmptyPacketEnd(mycat);
        return;
      } else if (mycat.shouldHandleContentOfFilename()) {
        MycatMonitor.onLoadDataLocalInFileContextStart(mycat);
        commandHandler.handleContentOfFilename(curPacket.readEOFStringBytes(), mycat);
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
          commandHandler.handleSleep(mycat);
          MycatMonitor.onSleepCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_QUIT: {
          MycatMonitor.onQuitCommandStart(mycat);
          curPacket.readByte();
          commandHandler.handleQuit(mycat);
          MycatMonitor.onQuitCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_QUERY: {
          MycatMonitor.onQueryCommandStart(mycat);
          curPacket.readByte();
          byte[] bytes = curPacket.readEOFStringBytes();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleQuery(bytes, mycat);
          MycatMonitor.onQueryCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_INIT_DB: {
          MycatMonitor.onInitDbCommandStart(mycat);
          curPacket.readByte();
          String schema = curPacket.readEOFString();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleInitDb(schema, mycat);
          MycatMonitor.onInitDbCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_PING: {
          MycatMonitor.onPingCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handlePing(mycat);
          MycatMonitor.onPingCommandEnd(mycat);
          break;
        }

        case MySQLCommandType.COM_FIELD_LIST: {
          MycatMonitor.onFieldListCommandStart(mycat);
          curPacket.readByte();
          String table = curPacket.readNULString();
          String field = curPacket.readEOFString();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleFieldList(table, field, mycat);
          MycatMonitor.onFieldListCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_SET_OPTION: {
          MycatMonitor.onSetOptionCommandStart(mycat);
          curPacket.readByte();
          boolean option = curPacket.readFixInt(2) == 1;
          mycat.resetCurrentProxyPayload();
          commandHandler.handleSetOption(option, mycat);
          MycatMonitor.onSetOptionCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_STMT_PREPARE: {
          MycatMonitor.onPrepareCommandStart(mycat);
          curPacket.readByte();
          byte[] bytes = curPacket.readEOFStringBytes();
          mycat.resetCurrentProxyPayload();
          commandHandler.handlePrepareStatement(bytes, mycat);
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
          commandHandler.handlePrepareStatementLongdata(statementId, paramId, data, mycat);
          MycatMonitor.onSendLongDataCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_STMT_EXECUTE: {
          MycatMonitor.onExecuteCommandStart(mycat);
          try {
            byte[] rawPayload = curPacket.getEOFStringBytes(curPacket.packetReadStartIndex());
            curPacket.readByte();
            long statementId = curPacket.readFixInt(4);
            byte flags = curPacket.readByte();
            long iteration = curPacket.readFixInt(4);
            assert iteration == 1;
            int numParams = commandHandler.getNumParamsByStatementId(statementId, mycat);

            int startIndex = curPacket.packetReadStartIndex();
            byte[] rest = curPacket.readEOFStringBytes();
            curPacket.packetReadStartIndex(startIndex);
            boolean newParameterBoundFlag = false;
            mycat.resetCurrentProxyPayload();
            commandHandler
                .handlePrepareStatementExecute(rawPayload, statementId, flags, numParams, rest,
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
          commandHandler.handlePrepareStatementClose(statementId, mycat);
          MycatMonitor.onCloseCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_STMT_FETCH: {
          MycatMonitor.onFetchCommandStart(mycat);
          curPacket.readByte();
          long statementId = curPacket.readFixInt(4);
          long row = curPacket.readFixInt(4);
          mycat.resetCurrentProxyPayload();
          commandHandler.handlePrepareStatementFetch(statementId, row, mycat);
          MycatMonitor.onFetchCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_STMT_RESET: {
          MycatMonitor.onResetCommandStart(mycat);
          curPacket.readByte();
          long statementId = curPacket.readFixInt(4);
          mycat.resetCurrentProxyPayload();
          commandHandler.handlePrepareStatementReset(statementId, mycat);
          MycatMonitor.onResetCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_CREATE_DB: {
          MycatMonitor.onCreateDbCommandStart(mycat);
          curPacket.readByte();
          String schema = curPacket.readEOFString();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleCreateDb(schema, mycat);
          MycatMonitor.onCreateDbCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_DROP_DB: {
          MycatMonitor.onDropDbCommandStart(mycat);
          curPacket.readByte();
          String schema = curPacket.readEOFString();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleDropDb(schema, mycat);
          MycatMonitor.onDropDbCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_REFRESH: {
          MycatMonitor.onRefreshCommandStart(mycat);
          curPacket.readByte();
          byte subCommand = curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleRefresh(subCommand, mycat);
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
              commandHandler.handleShutdown(shutdownType, mycat);
            } else {
              mycat.resetCurrentProxyPayload();
              commandHandler.handleShutdown(0, mycat);
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
          commandHandler.handleStatistics(mycat);
          MycatMonitor.onStatisticsCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_PROCESS_INFO: {
          MycatMonitor.onProcessInfoCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleProcessInfo(mycat);
          MycatMonitor.onProcessInfoCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_CONNECT: {
          MycatMonitor.onConnectCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleConnect(mycat);
          MycatMonitor.onConnectCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_PROCESS_KILL: {
          MycatMonitor.onProcessKillCommandStart(mycat);
          curPacket.readByte();
          long connectionId = curPacket.readFixInt(4);
          mycat.resetCurrentProxyPayload();
          commandHandler.handleProcessKill(connectionId, mycat);
          MycatMonitor.onProcessKillCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_DEBUG: {
          MycatMonitor.onDebugCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleDebug(mycat);
          MycatMonitor.onDebugCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_TIME: {
          MycatMonitor.onTimeCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleTime(mycat);
          MycatMonitor.onTimeCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_DELAYED_INSERT: {
          MycatMonitor.onDelayedInsertCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleTime(mycat);
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
            commandHandler
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
          commandHandler.handleResetConnection(mycat);
          MycatMonitor.onResetConnectionCommandEnd(mycat);
          break;
        }
        case MySQLCommandType.COM_DAEMON: {
          MycatMonitor.onDaemonCommandStart(mycat);
          curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleDaemon(mycat);
          MycatMonitor.onDaemonCommandEnd(mycat);
          break;
        }
        default: {
          assert false;
        }
      }
    }catch (Exception e){
      e.printStackTrace();
      mycat.setLastMessage(e);
      mycat.writeErrorEndPacketBySyncInProcessError();
      mycat.onHandlerFinishedClear();
    }finally {
      MycatMonitor.onCommandEnd(mycat);
    }
  }
}