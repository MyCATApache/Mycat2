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
package io.mycat.proxy;

import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.proxy.command.DirectPassthrouhCmd;
import io.mycat.proxy.command.MySQLPacketCommand;
import io.mycat.proxy.command.MySQLProxyCommand;
import io.mycat.proxy.packet.ErrorCode;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.RouteResult;
import io.mycat.sqlparser.util.ByteArrayView;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainMycatNIOHandler implements NIOHandler<MycatSession> {

  public static final MainMycatNIOHandler INSTANCE = new MainMycatNIOHandler();
  private static final Logger logger = LoggerFactory.getLogger(MainMycatNIOHandler.class);
  private static final String UNKNOWN_COMMAND = "Unknown command";

  @Override
  public void onSocketRead(MycatSession mycat) throws IOException {
    mycat.currentProxyBuffer().newBufferIfNeed();
    if (!mycat.readFromChannel()) {
      return;
    }
    if (!mycat.readMySQLPayloadFully()) {
      return;
    }
    MySQLPacket curPacket = mycat.currentPayload();
    byte head = curPacket.readByte();
    //command dispatcher
    switch (head) {
      case MySQLCommandType.COM_QUERY: {
        ByteArrayView byteArrayView = getByteArrayView(curPacket);
        doQuery(byteArrayView, mycat);
        mycat.resetCurrentPayload();
        if (mycat.getCurSQLCommand().handle(mycat)) {
          mycat.getCurSQLCommand().clearResouces(mycat, mycat.isClosed());
        }
        break;
      }
      case MySQLCommandType.COM_SLEEP: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_QUIT: {
        mycat.close(true, "COM_QUIT");
        break;
      }
      case MySQLCommandType.COM_INIT_DB: {
        String schema = curPacket.readEOFString();
        try {
          mycat.useSchema(schema);
          mycat.writeEndingOkPacket();
        } catch (Exception e) {
          mycat.writeEndingErrorPacket(e);
        }
        break;
      }
      case MySQLCommandType.COM_FIELD_LIST: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_CREATE_DB: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_DROP_DB: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_REFRESH: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_SHUTDOWN: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_STATISTICS: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_PROCESS_INFO: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_CONNECT: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_PROCESS_KILL: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_DEBUG: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_PING: {
        mycat.writeEndingOkPacket();
        break;
      }
      case MySQLCommandType.COM_TIME: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_DELAYED_INSERT: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_CHANGE_USER: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_RESET_CONNECTION: {
        try {
          mycat.resetSession();
          mycat.writeEndingOkPacket();
        } catch (Exception e) {
          mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        }
        break;
      }
      case MySQLCommandType.COM_DAEMON: {
        mycat.writeEndingErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR, UNKNOWN_COMMAND);
        break;
      }
      case MySQLCommandType.COM_SET_OPTION: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_PREPARE: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_EXECUTE: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_CLOSE: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      case MySQLCommandType.COM_STMT_RESET: {
        mycat.switchSQLCommand(MySQLPacketCommand.INSTANCE).handle(mycat);
        break;
      }
      default:
        throw new MycatExpection("unknown sql type");
    }
  }

  private ByteArrayView getByteArrayView(MySQLPacket curPacket) {
    return new ByteArrayView() {
            ByteBuffer buffer = curPacket.currentBuffer().currentByteBuffer().asReadOnlyBuffer();
            int length = curPacket.packetReadEndIndex() - curPacket.packetReadStartIndex();
            int offset = curPacket.packetReadStartIndex();

            @Override
            public byte getByte(int index) {
              return buffer.get(index);
            }

            @Override
            public int length() {
              return length;
            }

            @Override
            public void set(int index, byte value) {
              buffer.put(index, value);
            }

            @Override
            public void setOffset(int offset) {
              this.offset = offset;
            }

            @Override
            public int getOffset() {
              return this.offset;
            }
          };
  }

  private void doQuery(ByteArrayView sql, final MycatSession mycat)
      throws IOException {
    mycat.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
    RouteResult routeResult = mycat.route(sql);
    switch (routeResult.getRouteType()) {
      case PURE_QUERY_SINGLE_NODE:
        String dataNodeName = routeResult.getDataNodeName();
        mycat.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
        break;
      case COMPLEX_QUERY:
        mycat.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
        break;
      case OTHER:
        mycat.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
        break;
    }
  }

  @Override
  public void onSocketWrite(MycatSession mycat) throws IOException {
    mycat.writeToChannel();
  }

  @Override
  public void onWriteFinished(MycatSession mycat) throws IOException {
    MySQLProxyCommand command = mycat.getCurSQLCommand();
    if (command == null) {
      if (mycat.isResponseFinished()) {
        mycat.resetPacket();
        mycat.change2ReadOpts();
      } else {
        mycat.writeToChannel();
      }
    } else {
      if (command.onFrontWriteFinished(mycat)) {
        command.clearResouces(mycat, mycat.isClosed());
        mycat.switchSQLCommand(null);
        mycat.resetPacket();
      }
    }
  }

  @Override
  public void onSocketClosed(MycatSession session, boolean normal) {
    session.close(normal, "");
  }

}
