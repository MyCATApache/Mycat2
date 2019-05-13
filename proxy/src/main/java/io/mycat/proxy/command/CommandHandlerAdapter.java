/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.command;

import static io.mycat.proxy.command.LocalInFileRequestHandler.EMPTY_PACKET;
import static io.mycat.proxy.packet.AuthPacketImpl.calcLenencLength;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import java.util.HashMap;

/**
 * @author jamie12221
 * @date 2019-05-09 02:30
 **/
public class CommandHandlerAdapter {

  final CommandHandler commandHandler;

  public CommandHandlerAdapter(CommandHandler commandHandler) {
    this.commandHandler = commandHandler;
  }

  public void handle(MycatSession mycat) {
    MySQLPacket curPacket = mycat.currentProxyPayload();
    if (mycat.getLocalInFileState() == LocalInFileRequestHandler.CONTENT_OF_FILE) {
      byte[] bytes = curPacket.readEOFStringBytes();
      mycat.resetCurrentProxyPayload();
      commandHandler.handleContentOfFilename(bytes, mycat);
      mycat.setLocalInFileState(EMPTY_PACKET);
      return;
    } else if (mycat.getLocalInFileState() == EMPTY_PACKET) {
      mycat.resetCurrentProxyPayload();
      commandHandler.handleContentOfFilenameEmptyOk();
      mycat.setLocalInFileState(LocalInFileRequestHandler.COM_QUERY);
      return;
    }
    byte head = curPacket.readByte();
    switch (head) {
      case MySQLCommandType.COM_SLEEP: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleSleep(mycat);
        break;
      }
      case MySQLCommandType.COM_QUIT: {
        commandHandler.handleQuit(mycat);
        break;
      }
      case MySQLCommandType.COM_QUERY: {
        byte[] bytes = curPacket.readEOFStringBytes();
        mycat.resetCurrentProxyPayload();
        commandHandler.handleQuery(bytes, mycat);
        break;
      }
      case MySQLCommandType.COM_INIT_DB: {
        String schema = curPacket.readEOFString();
        mycat.resetCurrentProxyPayload();
        commandHandler.handleInitDb(schema, mycat);
        break;
      }
      case MySQLCommandType.COM_PING: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handlePing(mycat);
        break;
      }

      case MySQLCommandType.COM_FIELD_LIST: {
        String table = curPacket.readNULString();
        String field = curPacket.readEOFString();
        mycat.resetCurrentProxyPayload();
        commandHandler.handleFieldList(table, field, mycat);
        break;
      }
      case MySQLCommandType.COM_SET_OPTION: {
        boolean option = curPacket.readFixInt(2) == 1;
        mycat.resetCurrentProxyPayload();
        commandHandler.handleSetOption(option, mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_PREPARE: {
        byte[] bytes = curPacket.readEOFStringBytes();
        mycat.resetCurrentProxyPayload();
        commandHandler.handlePrepareStatement(bytes, mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
        long statementId = curPacket.readFixInt(4);
        long paramId = curPacket.readFixInt(2);
        byte[] data = curPacket.readEOFStringBytes();
        mycat.resetCurrentProxyPayload();
        commandHandler.handlePrepareStatementLongdata(statementId, paramId, data, mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_EXECUTE: {
        long statementId = curPacket.readFixInt(4);
        byte flags = curPacket.readByte();
        long iteration = curPacket.readFixInt(4);
        assert iteration == 1;
        int numParams = mycat.getNumParamsByStatementId(statementId);
        if (numParams > 0) {
          int length = (numParams + 7) / 8;
          byte[] nullMap = curPacket.readBytes(length);
          byte newParamsBoundFlag = curPacket.readByte();
          if (newParamsBoundFlag == 1) {
            byte[] typeList = new byte[numParams];
            byte[] fieldList = new byte[numParams];
            for (int i = 0; i < numParams; i++) {
              typeList[i] = curPacket.readByte();
              fieldList[i] = curPacket.readByte();
            }
            mycat.resetCurrentProxyPayload();
            commandHandler
                .handlePrepareStatementExecute(statementId, flags, numParams, nullMap, true,
                    typeList, fieldList,
                    mycat);
            break;
          } else {
            mycat.resetCurrentProxyPayload();
            commandHandler
                .handlePrepareStatementExecute(statementId, flags, numParams, nullMap, false, null,
                    null, mycat);
            break;
          }
        } else {
          mycat.resetCurrentProxyPayload();
          commandHandler
              .handlePrepareStatementExecute(statementId, flags, numParams, null, false, null, null,
                  mycat);
          break;
        }
      }
      case MySQLCommandType.COM_STMT_CLOSE: {
        long statementId = curPacket.readFixInt(4);
        mycat.resetCurrentProxyPayload();
        commandHandler.handlePrepareStatementClose(statementId, mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_RESET: {
        long statementId = curPacket.readFixInt(4);
        mycat.resetCurrentProxyPayload();
        commandHandler.handlePrepareStatementReset(statementId, mycat);
        break;
      }
      case MySQLCommandType.COM_CREATE_DB: {
        String schema = curPacket.readEOFString();
        mycat.resetCurrentProxyPayload();
        commandHandler.handleCreateDb(schema, mycat);
        break;
      }
      case MySQLCommandType.COM_DROP_DB: {
        String schema = curPacket.readEOFString();
        mycat.resetCurrentProxyPayload();
        commandHandler.handleDropDb(schema, mycat);
        break;
      }
      case MySQLCommandType.COM_REFRESH: {
        byte subCommand = curPacket.readByte();
        mycat.resetCurrentProxyPayload();
        commandHandler.handleRefresh(subCommand, mycat);
        break;
      }
      case MySQLCommandType.COM_SHUTDOWN: {
        if (!curPacket.readFinished()) {
          byte shutdownType = curPacket.readByte();
          mycat.resetCurrentProxyPayload();
          commandHandler.handleShutdown(shutdownType, mycat);
        } else {
          mycat.resetCurrentProxyPayload();
          commandHandler.handleShutdown(0, mycat);
        }
        break;
      }
      case MySQLCommandType.COM_STATISTICS: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleStatistics(mycat);
        break;
      }
      case MySQLCommandType.COM_PROCESS_INFO: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleProcessInfo(mycat);
        break;
      }
      case MySQLCommandType.COM_CONNECT: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleConnect(mycat);
        break;
      }
      case MySQLCommandType.COM_PROCESS_KILL: {
        long connectionId = curPacket.readFixInt(4);
        mycat.resetCurrentProxyPayload();
        commandHandler.handleProcessKill(connectionId, mycat);
        break;
      }
      case MySQLCommandType.COM_DEBUG: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleDebug(mycat);
        break;
      }
      case MySQLCommandType.COM_TIME: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleTime(mycat);
        break;
      }
      case MySQLCommandType.COM_DELAYED_INSERT: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleTime(mycat);
        break;
      }
      case MySQLCommandType.COM_CHANGE_USER: {
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
        break;
      }
      case MySQLCommandType.COM_RESET_CONNECTION: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleResetConnection(mycat);
        break;
      }
      case MySQLCommandType.COM_DAEMON: {
        mycat.resetCurrentProxyPayload();
        commandHandler.handleDaemon(mycat);
        break;
      }
      default: {
        assert false;
      }
    }
  }
}
