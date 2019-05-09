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

import static io.mycat.sqlparser.util.BufferSQLContext.ALTER_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.BEGIN_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.CALL_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.COMMIT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.CREATE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.DELETE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.DESCRIBE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.DROP_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.EXPLAIN_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.GRANT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.HANDLER_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.HELP_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.INSERT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.KILL_QUERY_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.KILL_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.LOAD_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.LOCK_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.PARTITION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.RENAME_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.REPLACE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.REVOKE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.ROLLBACK_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SAVEPOINT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_FOR_UPDATE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_INTO_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_AUTOCOMMIT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.START_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.TRUNCATE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.UPDATE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.USE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.XA_SQL;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.config.schema.TableDefConfig.MycatTableType;
import io.mycat.proxy.MultiMySQLQueryTask;
import io.mycat.proxy.MySQLProxyHandler;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.executer.RouteResultExecuter;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.payload.MySQLPacketUtil;
import io.mycat.proxy.session.MySQLServerSession.WriteHandler;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ResultRoute;
import io.mycat.router.util.RouterUtil;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author jamie12221
 * @date 2019-05-09 02:30
 **/
public enum CommandHandler {
  INSTANCE;
  private static final String UNKNOWN_COMMAND = "Unknown command";

  public void query(MycatSession mycat, String sql) throws IOException {
    mycat.setResponseFinished(false);
    MycatReactorThread thread = mycat.getMycatReactorThread();
    MycatRouter router = thread.getRouter();
    BufferSQLContext bufferSQLContext = router.simpleParse(sql);
    sql = RouterUtil.removeSchema(sql, mycat.getSchema().getSchemaName());
    byte[] sqlBytes = MySQLPacketUtil
                          .generateRequestPacket(MySQLCommandType.COM_QUERY, sql.getBytes());
    mycat.rebuildProxyRequest(sqlBytes);
    String tableName = bufferSQLContext.getTableName(0);
    byte sqlType = bufferSQLContext.getSQLType();
    switch (sqlType) {
      case CREATE_SQL: {

      }//TODO 进一步细化。 区分
      case ALTER_SQL: {

      }
      case DROP_SQL: {

      }
      case TRUNCATE_SQL: {

      }
      case RENAME_SQL:
        break;
      case USE_SQL: {
        String schemaName = bufferSQLContext.getSchemaName(0);
        mycat.switchWriteHandler(WriteHandler.INSTANCE);
        mycat.useSchema(schemaName);
        mycat.writeOkEndPacket();
        break;
      }
      case SHOW_DB_SQL: {
        mycat.switchWriteHandler(WriteHandler.INSTANCE);
        mycat.writeColumnCount(1);
        mycat.writeColumnDef("Dababase", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        mycat.writeColumnEndPacket();
        MycatRouterConfig config = router.getConfig();
        Collection<MycatSchema> schemaList = config.getSchemaList();
        for (MycatSchema schema : schemaList) {
          String schemaName = schema.getSchemaName();
          mycat.writeTextRowPacket(new byte[][]{schemaName.getBytes(mycat.charset())});
        }
        mycat.writeRowEndPacket(false);
        break;
      }
      case SHOW_TB_SQL: {
        String schemaName =
            bufferSQLContext.getSchemaCount() == 1 ? bufferSQLContext.getSchemaName(0)
                : mycat.getSchema().getSchemaName();
        mycat.switchWriteHandler(WriteHandler.INSTANCE);
        mycat.writeColumnCount(2);
        mycat.writeColumnDef("Tables in " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        mycat.writeColumnDef("Table_type " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
        mycat.writeColumnEndPacket();
        MycatRouterConfig config = router.getConfig();
        MycatSchema schema = config.getSchemaBySchemaName(schemaName);
        byte[] basetable = "BASE TABLE".getBytes(mycat.charset());
        for (String name : schema.getMycatTables().keySet()) {
          mycat.writeTextRowPacket(new byte[][]{name.getBytes(), basetable});
        }
        mycat.writeRowEndPacket(false);
        break;
      }
      case SELECT_SQL: {
        if (bufferSQLContext.isSimpleSelect() && bufferSQLContext.getTableCount() == 1) {

          if (mycat.getSchema().existTable(tableName)) {
            try {
              ResultRoute resultRoute = router.enterRoute(mycat.getSchema(), bufferSQLContext, sql);
              resultRoute.accept(RouteResultExecuter.INSTANCE, mycat);
              break;//路由出错走默认节点
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }
      case SHOW_SQL: {
        try {
          if (bufferSQLContext.getTableCount() == 1 && tableName != null) {
            String s = mycat.getSchema().getTableByTableName(tableName).getDataNodes().get(0);
            mycat.setDataNode(s);
            MySQLProxyHandler.INSTANCE.handle(mycat);
            return;
          }
        } catch (Exception e) {

        }
      }
      case SET_SQL: {

      }
      case PARTITION_SQL: {

      }
      //DML
      case UPDATE_SQL: {

      }
      case DELETE_SQL: {

      }
      case INSERT_SQL: {

      }
      case REPLACE_SQL: {

      }
      case CALL_SQL: {

      }
      case EXPLAIN_SQL: {

      }

      case DESCRIBE_SQL: {

      }
      case HANDLER_SQL: {

      }
      case LOAD_SQL: {

      }
      case HELP_SQL: {

      }

      //DCL
      case GRANT_SQL: {

      }
      case REVOKE_SQL: {

      }
      case KILL_SQL: {

      }
      case KILL_QUERY_SQL: {

      }
      //TCL
      case START_SQL: {

      }
      case BEGIN_SQL: {

      }

      case TRANSACTION_SQL: {

      }
      case SAVEPOINT_SQL: {

      }
      case ROLLBACK_SQL: {

      }
      case SET_TRANSACTION_SQL: {

      }
      case LOCK_SQL: {

      }
      case XA_SQL: {

      }
      case SET_AUTOCOMMIT_SQL: {

      }
      case COMMIT_SQL: {

      }
      case SELECT_INTO_SQL: {

      }
      case SELECT_FOR_UPDATE_SQL: {

      }
      default: {
        try {
          if (tableName != null) {
            MycatTable table = mycat.getSchema().getTableByTableName(tableName);
            if (table!=null&&table.getType() == MycatTableType.GLOBAL) {
              List<String> dataNodes = table.getDataNodes();
              List<String> strings = dataNodes.subList(0, dataNodes.size() - 1);
              mycat.setDataNode(dataNodes.get(dataNodes.size() - 1));
              new MultiMySQLQueryTask(mycat, MySQLPacketUtil.generateComQueryPacket( sql),
                  strings,
                  (session, sender, success, result, attr) -> {
                    try {
                      mycat.rebuildProxyRequest(sqlBytes);
                      MySQLProxyHandler.INSTANCE.handle(mycat);
                    } catch (Exception e) {
                      mycat.resetPacket();
                      mycat.writeErrorEndPacket();
                    }
                  });
              return;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        {
          String defaultDataNode = mycat.getSchema().getDefaultDataNode();
          mycat.rebuildProxyRequest(sqlBytes);
          mycat.setDataNode(defaultDataNode);
          MySQLProxyHandler.INSTANCE.handle(mycat);
        }
      }
    }
  }


  public void handle(MycatSession mycat) throws IOException {
    MySQLPacket curPacket = mycat.currentProxyPayload();
    byte head = curPacket.readByte();
    switch (head) {
      case MySQLCommandType.COM_SLEEP: {
        mycat.resetCurrentProxyPayload();
        mycat.writeErrorEndPacket();
        break;
      }
      case MySQLCommandType.COM_QUIT: {
        mycat.resetCurrentProxyPayload();
        mycat.close(true, "COM_QUIT");
        break;
      }
      case MySQLCommandType.COM_QUERY: {
        String sql = curPacket.readEOFString();
        mycat.resetCurrentProxyPayload();
        query(mycat, sql);
        break;
      }
      case MySQLCommandType.COM_INIT_DB: {
        String schema = curPacket.readEOFString();
        mycat.resetCurrentProxyPayload();
        try {
          mycat.useSchema(schema);
          mycat.writeOkEndPacket();
        } catch (Exception e) {
          mycat.writeErrorEndPacket();
        }
        break;
      }
      case MySQLCommandType.COM_PING: {
        mycat.resetCurrentProxyPayload();
        mycat.writeOkEndPacket();
        break;
      }

      case MySQLCommandType.COM_FIELD_LIST: {
        mycat.resetCurrentProxyPayload();
        mycat.writeErrorEndPacket();
        break;
      }
      case MySQLCommandType.COM_SET_OPTION:
      case MySQLCommandType.COM_STMT_PREPARE:
      case MySQLCommandType.COM_STMT_SEND_LONG_DATA:
      case MySQLCommandType.COM_STMT_EXECUTE:
      case MySQLCommandType.COM_STMT_CLOSE:
      case MySQLCommandType.COM_STMT_RESET: {
        mycat.resetCurrentProxyPayload();
        mycat.writeErrorEndPacket();
      }
      case MySQLCommandType.COM_CREATE_DB:
      case MySQLCommandType.COM_DROP_DB:
      case MySQLCommandType.COM_REFRESH:
      case MySQLCommandType.COM_SHUTDOWN:
      case MySQLCommandType.COM_STATISTICS:
      case MySQLCommandType.COM_PROCESS_INFO:
      case MySQLCommandType.COM_CONNECT:
      case MySQLCommandType.COM_PROCESS_KILL:
      case MySQLCommandType.COM_DEBUG:
      case MySQLCommandType.COM_TIME:
      case MySQLCommandType.COM_DELAYED_INSERT:
      case MySQLCommandType.COM_CHANGE_USER:
      case MySQLCommandType.COM_RESET_CONNECTION:
      case MySQLCommandType.COM_DAEMON:
      default:
        mycat.resetCurrentProxyPayload();
        mycat.writeErrorEndPacket();
    }

  }

}
