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
package io.mycat.command;

import static io.mycat.sqlparser.util.BufferSQLContext.DESCRIBE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_VARIABLES;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_AUTOCOMMIT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_CHARSET;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_CHARSET_RESULT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_NET_WRITE_TIMEOUT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_SQL_SELECT_LIMIT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_VARIABLES_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_WARNINGS;
import static io.mycat.sqlparser.util.BufferSQLContext.USE_SQL;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLIsolationLevel;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.handler.backend.MySQLQuery;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ResultRoute;
import io.mycat.router.routeResult.GlobalTableWriteResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.util.RouterUtil;
import io.mycat.security.MycatUser;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221 date 2019-05-17 17:37
 **/
public interface QueryHandler {

  static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);

  MycatRouter router();

  default void doQuery(byte[] sqlBytes, MycatSession mycat) {
    MycatRouterConfig routerConfig = ProxyRuntime.INSTANCE.getRouterConfig();

    /**
     * 获取默认的schema
     */
    MycatSchema useSchema = mycat.getSchema();
    if (useSchema == null) {
      useSchema = router().getDefaultSchema();
    }
    MycatUser user = mycat.getUser();
    String orgin = new String(sqlBytes);
    MycatMonitor.onOrginSQL(mycat, orgin);
    orgin = routerConfig.getSqlInterceptor().interceptSQL(orgin);
    BufferSQLContext sqlContext = router().simpleParse(orgin);
    String sql = RouterUtil.removeSchema(orgin, useSchema.getSchemaName());
    sql = sql.trim();
    byte sqlType = sqlContext.getSQLType();

    if (!user.checkSQL(sqlType, sql, Collections.EMPTY_SET)) {
      mycat.setLastMessage("Because the security policy is not enforceable");
      mycat.writeErrorEndPacket();
      return;
    }
    if (mycat.isBindMySQLSession()) {
      MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql),
          mycat.getMySQLSession().getDataNode().getName(), null, ResponseType.QUERY);
      return;
    }
    try {
      switch (sqlType) {
        case USE_SQL: {
          String schemaName = sqlContext.getSchemaName(0);
          useSchema(mycat, schemaName);
          break;
        }
        case SET_AUTOCOMMIT_SQL: {
          Boolean autocommit = sqlContext.isAutocommit();
          if (autocommit == null) {
            mycat.setLastMessage("set autocommit fail!");
            mycat.writeErrorEndPacket();
            return;
          } else {
            mycat.setAutoCommit(autocommit ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF);
            mycat.writeOkEndPacket();
            return;
          }
        }
        case SET_CHARSET: {
          String charset = sqlContext.getCharset();
          mycat.setCharset(charset);
          mycat.writeOkEndPacket();
          return;
        }
        case SET_SQL_SELECT_LIMIT: {
          mycat.setSelectLimit(sqlContext.getSqlSelectLimit());
          mycat.writeOkEndPacket();
          return;
        }
        case SET_NET_WRITE_TIMEOUT: {
          mycat.setNetWriteTimeout(sqlContext.getNetWriteTimeout());
          mycat.writeOkEndPacket();
          return;
        }
        case SET_CHARSET_RESULT: {
          String charsetSetResult = sqlContext.getCharsetSetResult();
          mycat.setCharsetSetResult(charsetSetResult);
          mycat.writeOkEndPacket();
          return;
        }
        case SET_TRANSACTION_SQL: {
          if (sqlContext.isAccessMode()) {
            LOGGER.warn("ignore {} and send ok", sql);
            mycat.writeOkEndPacket();
            return;
          }
          if (sqlContext.getTransactionLevel() == MySQLIsolationLevel.GLOBAL) {
            LOGGER.warn("unsupport global send error", sql);
            mycat.setLastMessage("unsupport global level");
            mycat.writeErrorEndPacket();
            return;
          }
          MySQLIsolation isolation = sqlContext.getIsolation();
          if (isolation == null) {
            mycat.setLastMessage("set transaction fail!");
            mycat.writeErrorEndPacket();
            return;
          }
          mycat.setIsolation(isolation);
          mycat.writeOkEndPacket();
          return;
        }
        case SHOW_DB_SQL: {
          MycatRouterConfig config = router().getConfig();
          showDb(mycat, config.getSchemaList());
          break;
        }
        case SHOW_TB_SQL: {
          String schemaName =
              sqlContext.getSchemaCount() == 1 ? sqlContext.getSchemaName(0)
                  : useSchema.getSchemaName();
          showTable(mycat, schemaName);
          break;
        }
        case DESCRIBE_SQL:
//          mycat.setLastMessage("unsupport desc");
//          mycat.writeErrorEndPacket();
//          return;
        case SHOW_SQL:
          String defaultDataNode = useSchema.getDefaultDataNode();
          MySQLTaskUtil
              .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql), defaultDataNode, null,
                  ResponseType.QUERY);
          return;
        case SHOW_VARIABLES_SQL: {
          mycat.writeColumnCount(2);
          mycat.writeColumnDef("Variable_name", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnDef("Value", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnEndPacket();

          Set<Entry<String, String>> entries = ProxyRuntime.INSTANCE.getVariables().entries();
          for (Entry<String, String> entry : entries) {
            mycat.writeTextRowPacket(
                new byte[][]{mycat.encode(entry.getKey()), mycat.encode(entry.getValue())});
          }
          mycat.writeRowEndPacket(false, false);
          return;
        }
        case SHOW_WARNINGS: {
          mycat.writeColumnCount(3);
          mycat.writeColumnDef("Level", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnDef("Code", MySQLFieldsType.FIELD_TYPE_LONG_BLOB);
          mycat.writeColumnDef("CMessage", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
          mycat.writeColumnEndPacket();
          mycat.writeRowEndPacket(false, false);
          return;
        }
        case SELECT_VARIABLES: {
          if (sqlContext.isSelectAutocommit()) {
            mycat.writeColumnCount(1);
            mycat.writeColumnDef("@@session.autocommit", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
            mycat.writeColumnEndPacket();
            mycat.writeTextRowPacket(new byte[][]{mycat.encode(mycat.getAutoCommit().getText())});
            mycat.writeRowEndPacket(false, false);
            return;
          } else if (sqlContext.isSelectTxIsolation()) {
            mycat.writeColumnCount(1);
            mycat.writeColumnDef("@@session.tx_isolation", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
            mycat.writeColumnEndPacket();
            mycat.writeTextRowPacket(new byte[][]{mycat.encode(mycat.getIsolation().getText())});
            mycat.writeRowEndPacket(false, false);
            return;
          } else if (sqlContext.isSelectTranscationReadOnly()) {
            mycat.writeColumnCount(1);
            mycat.writeColumnDef("@@session.transaction_read_only",
                MySQLFieldsType.FIELD_TYPE_LONGLONG);
            mycat.writeColumnEndPacket();
            mycat.writeTextRowPacket(new byte[][]{mycat.encode(mycat.getIsolation().getText())});
            mycat.writeRowEndPacket(false, false);
            return;
          }
          LOGGER.warn("maybe unsupported  sql:{}", sql);
        }
        case SELECT_SQL: {
          if (sqlContext.isSimpleSelect()) {
            ResultRoute resultRoute = router().enterRoute(useSchema, sqlContext, sql);
            if (resultRoute != null) {
              switch (resultRoute.getType()) {
                case ONE_SERVER_RESULT_ROUTE:
                  OneServerResultRoute route = (OneServerResultRoute) resultRoute;
                  MySQLQuery query = new MySQLQuery();
                  query.setIds(null);
                  query.setRunOnMaster(resultRoute.isRunOnMaster(false));
                  query.setStrategy(ProxyRuntime.INSTANCE
                      .getLoadBalanceByBalanceName(resultRoute.getBalance()));
                  MySQLTaskUtil
                      .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(route.getSql()),
                          route.getDataNode(), query, ResponseType.QUERY);
                  return;
              }
            }
            mycat.setLastMessage("unsupport sql");
            mycat.writeErrorEndPacket();
            return;//路由出错走默认节点
          }
        }

        default:
          switch (useSchema.getSchema()) {
            case DB_IN_ONE_SERVER:
              MySQLTaskUtil
                  .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql),
                      useSchema.getDefaultDataNode(), null, ResponseType.QUERY);
              return;
            case DB_IN_MULTI_SERVER:
            case ANNOTATION_ROUTE:
            case SQL_PARSE_ROUTE:
              if (sqlContext.getSQLType() != 0 & sqlContext.getTableCount() != 1) {
                mycat.setLastMessage("unsupport sql");
                mycat.writeErrorEndPacket();
                return;
              }
          }

          ResultRoute resultRoute = router().enterRoute(useSchema, sqlContext, sql);
          if (resultRoute == null) {
            mycat.writeOkEndPacket();
            return;
          }
          switch (resultRoute.getType()) {
            case ONE_SERVER_RESULT_ROUTE: {
              OneServerResultRoute resultRoute1 = (OneServerResultRoute) resultRoute;
              MySQLTaskUtil
                  .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(resultRoute1.getSql()),
                      resultRoute1.getDataNode(), null, ResponseType.QUERY);
              break;
            }
            case GLOBAL_TABLE_WRITE_RESULT_ROUTE: {
              GlobalTableWriteResultRoute globalTableWriteResultRoute = (GlobalTableWriteResultRoute) resultRoute;
              String sql1 = globalTableWriteResultRoute.getSql();
              String master = globalTableWriteResultRoute.getMaster();
              Collection<String> dataNodes = globalTableWriteResultRoute.getDataNodes();
              mycat.setLastMessage("unsupport sql");
              mycat.writeErrorEndPacket();
//              mycat.proxyUpdateMultiBackends(MySQLPacketUtil.generateComQuery(sql1), master,
//                  dataNodes, new AsyncTaskCallBack<MycatSessionView>() {
//                    @Override
//                    public void finished(MycatSessionView session, Object sender, boolean success,
//                        Object result, Object attr) {
//                      if (success) {
//                        System.out.println("success full");
//                      } else {
//                        session.setLastMessage(result.toString());
//                        session.writeErrorEndPacket();
//                      }
//                    }
//                  });
              return;
            }
            default:
              mycat.setLastMessage("unsupport sql");
              mycat.writeErrorEndPacket();
          }

      }
    } catch (Exception e) {
      mycat.setLastMessage(e);
      mycat.writeErrorEndPacket();
    }
  }

  default void showDb(MycatSession mycat, Collection<MycatSchema> schemaList) {
    mycat.writeColumnCount(1);
    byte[] bytes = MySQLPacketUtil
        .generateColumnDef("information_schema","SCHEMATA","SCHEMATA","Database","SCHEMA_NAME",MySQLFieldsType.FIELD_TYPE_VAR_STRING,
            0x1,0,mycat.charsetIndex(),192, Charset.defaultCharset());
    mycat.writeBytes(bytes);
    mycat.writeColumnEndPacket();
    for (MycatSchema schema : mycat.getUser().getSchemas().values()) {
      String schemaName = schema.getSchemaName();
      mycat.writeTextRowPacket(new byte[][]{schemaName.getBytes(mycat.charset())});
    }
    mycat.countDownResultSet();
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  default void showTable(MycatSession mycat, String schemaName) {
    Collection<String> tableName = router().getConfig().getSchemaBySchemaName(schemaName)
        .getMycatTables().keySet();
    mycat.writeColumnCount(2);
    mycat.writeColumnDef("Tables in " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnDef("Table_type " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnEndPacket();
    MycatRouterConfig config = router().getConfig();
    MycatSchema schema = config.getSchemaBySchemaName(schemaName);
    byte[] basetable = mycat.encode("BASE TABLE");
    for (String name : schema.getMycatTables().keySet()) {
      mycat.writeTextRowPacket(new byte[][]{mycat.encode(name), basetable});
    }
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  default void useSchema(MycatSession mycat, String schemaName) {
    MycatSchema schema = router().getConfig().getSchemaBySchemaName(schemaName);
    mycat.useSchema(schema);
    mycat.writeOkEndPacket();
  }
}
