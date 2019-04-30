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

import io.mycat.proxy.command.MySQLCommandType;
import io.mycat.proxy.command.MySQLProxyCommand;
import io.mycat.proxy.packet.ErrorCode;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MycatSession;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainMycatNIOHandler implements NIOHandler<MycatSession> {

  public static final MainMycatNIOHandler INSTANCE = new MainMycatNIOHandler();
  private static final Logger logger = LoggerFactory.getLogger(MainMycatNIOHandler.class);

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
        String sql = curPacket.readEOFString();
        doQuery(curPacket.currentBuffer().currentByteBuffer().asCharBuffer(),mycat);
        if (mycat.getCurSQLCommand().procssSQL(mycat)) {
          mycat.getCurSQLCommand().clearResouces(mycat, mycat.isClosed());
        }
        break;
      }
      case MySQLCommandType.COM_SLEEP: {
        mycat.writeErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR,"Unknown command");
        break;
      }
      case MySQLCommandType.COM_QUIT: {
        mycat.writeErrorPacket(ErrorCode.ER_UNKNOWN_COM_ERROR,"Unknown command");
        break;
      }
      case MySQLCommandType.COM_INIT_DB: {
        break;
      }

      case MySQLCommandType.COM_FIELD_LIST: {
        break;
      }
      case MySQLCommandType.COM_CREATE_DB: {
        break;
      }
      case MySQLCommandType.COM_DROP_DB: {
        break;
      }
      case MySQLCommandType.COM_REFRESH: {
        break;
      }
      case MySQLCommandType.COM_SHUTDOWN: {
        break;
      }
      case MySQLCommandType.COM_STATISTICS: {
        break;
      }
      case MySQLCommandType.COM_PROCESS_INFO: {
        break;
      }
      case MySQLCommandType.COM_CONNECT_OUT: {
        break;
      }
      case MySQLCommandType.COM_CONNECT: {
        break;
      }
      case MySQLCommandType.COM_PROCESS_KILL: {
        break;
      }
      case MySQLCommandType.COM_DEBUG: {
        break;
      }
      case MySQLCommandType.COM_PING: {
        break;
      }
      case MySQLCommandType.COM_TIME: {
        break;
      }
      case MySQLCommandType.COM_DELAYED_INSERT: {
        break;
      }
      case MySQLCommandType.COM_CHANGE_USER: {
        break;
      }
      case MySQLCommandType.COM_RESET_CONNECTION: {
        break;
      }
      case MySQLCommandType.COM_DAEMON: {
        break;
      }
      case MySQLCommandType.COM_SET_OPTION: {
        break;
      }
      case MySQLCommandType.COM_STMT_PREPARE: {
        break;
      }
      case MySQLCommandType.COM_STMT_SEND_LONG_DATA: {
        break;
      }
      case MySQLCommandType.COM_STMT_EXECUTE: {
        break;
      }
      case MySQLCommandType.COM_STMT_CLOSE: {
        break;
      }
      case MySQLCommandType.COM_STMT_RESET: {
        break;
      }
    }

  }

  private void doQuery(CharSequence sql,final MycatSession mycat) throws IOException {
    mycat.switchSQLCommand(DirectPassthrouhCmd.INSTANCE);
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
