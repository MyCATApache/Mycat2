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

import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.DELETE_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.DESCRIBE_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.INSERT_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.LOAD_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SELECT_FOR_UPDATE_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SELECT_VARIABLES;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SET_AUTOCOMMIT_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SET_CHARSET;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SET_CHARSET_RESULT;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SET_NET_WRITE_TIMEOUT;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SET_SQL_SELECT_LIMIT;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SHOW_VARIABLES_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.SHOW_WARNINGS;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.UPDATE_SQL;
import static io.mycat.sqlparser.util.simpleParser.BufferSQLContext.USE_SQL;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLIsolationLevel;
import io.mycat.beans.resultset.MycatResponse;
import io.mycat.config.schema.SchemaType;
import io.mycat.grid.MycatRouterResponse;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.PlugRuntime;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.SQLExecuterWriter;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ProxyRouteResult;
import io.mycat.router.util.RouterUtil;
import io.mycat.security.MycatUser;
import io.mycat.sequenceModifier.ModifyCallback;
import io.mycat.sequenceModifier.SequenceModifier;
import io.mycat.sqlparser.util.simpleParser.BufferSQLContext;

/**
 * @author jamie12221 date 2019-05-17 17:37
 **/
public class ProxyQueryHandler {

  static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ProxyQueryHandler.class);
  static final MycatLogger IGNORED_SQL_LOGGER = MycatLoggerFactory.getLogger("IGNORED_SQL_LOGGER");
  final MycatRouter router;
  final private ProxyRuntime runtime;

  public ProxyQueryHandler(MycatRouter router, ProxyRuntime runtime) {
    this.router = router;
    this.runtime = runtime;
  }

  public void doQuery(MycatSchema schema, byte[] sqlBytes, MycatSession mycat) {
    final MycatSchema useSchema = schema == null ? router.getDefaultSchema() : schema;
    final MycatUser user = mycat.getUser();
    final String orgin = new String(sqlBytes);
    MycatMonitor.onOrginSQL(mycat, orgin);
    final String sql = RouterUtil.removeSchema(orgin, useSchema.getSchemaName()).trim();
    final BufferSQLContext sqlContext = router.simpleParse(sql);
    byte sqlType = sqlContext.getSQLType();
    if (mycat.isBindMySQLSession()) {
      MySQLTaskUtil.proxyBackend(mycat, sql,
          mycat.getMySQLSession().getDataNode().getName(), null);
      return;
    }
    try {
      switch (sqlType) {
        case USE_SQL: {
          useSchema(mycat, sqlContext);
          break;
        }
        case SET_AUTOCOMMIT_SQL: {
          setAutocommit(mycat, sqlContext);
          return;
        }
        case SET_CHARSET: {
          setCharset(mycat, sqlContext);
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
          setTranscation(mycat, sql, sqlContext);
          return;
        }
        case SHOW_DB_SQL: {
          MycatRouterConfig config = router.getConfig();
          SQLExecuterWriter.writeToMycatSession(mycat,  MycatRouterResponse.showDb(mycat, config.getSchemaList()));
          break;
        }
        case SHOW_TB_SQL: {
          String schemaName =
              sqlContext.getSchemaCount() == 1 ? sqlContext.getSchemaName(0)
                  : useSchema.getSchemaName();
          SQLExecuterWriter.writeToMycatSession(mycat,MycatRouterResponse.showTable(router,mycat,schemaName));
          break;
        }
        case DESCRIBE_SQL:
          mycat.writeOkEndPacket();
          return;
        case SHOW_SQL:
          String defaultDataNode = useSchema.getDefaultDataNode();
          if (defaultDataNode == null) {
            throw new MycatException("show sql:{} can not route", sql);
          }
          MySQLTaskUtil
              .proxyBackend(mycat, sql, defaultDataNode, null);
          return;
        case SHOW_VARIABLES_SQL: {
          MySQLTaskUtil
              .proxyBackend(mycat, sql, router.getRandomDataNode(schema), null);
          return;
        }
        case SHOW_WARNINGS: {
          SQLExecuterWriter.writeToMycatSession(mycat,MycatRouterResponse.showWarnnings(mycat));
          return;
        }
        case SELECT_VARIABLES: {
          MycatResponse sqlExecuter = MycatRouterResponse.selectVariables(mycat, sqlContext);
          if (sqlExecuter != null) {
            SQLExecuterWriter.writeToMycatSession(mycat,sqlExecuter);
            return;
          }
          IGNORED_SQL_LOGGER.warn("ignore:{}",sql);
          execute(mycat,schema,sql,sqlContext,sqlType);
          return;
        }
        case LOAD_SQL: {
          IGNORED_SQL_LOGGER
              .warn("Use annotations to specify loadata data nodes whenever possible !");
        }

        case INSERT_SQL:
        case UPDATE_SQL:
        case DELETE_SQL:
        case SELECT_FOR_UPDATE_SQL:
        case SELECT_SQL:{
          if (router.existTable(schema.getSchemaName(), sqlContext.getTableName(0))) {
            if (useSchema.getSchemaType() == SchemaType.ANNOTATION_ROUTE) {
              SequenceModifier modifier = useSchema.getModifier();
              if (modifier != null) {
                modifier.modify(useSchema.getSchemaName(), sql, new ModifyCallback() {
                  @Override
                  public void onSuccessCallback(String sql) {
                    execute(mycat, useSchema, sql, sqlContext, sqlType);
                  }

                  @Override
                  public void onException(Exception e) {
                    mycat.setLastMessage(e);
                    mycat.writeErrorEndPacket();
                  }
                });
                return;
              }
            }
            execute(mycat, useSchema, sql, sqlContext, sqlType);
            return;
          } else if (sqlType == SELECT_SQL || sqlType == SELECT_FOR_UPDATE_SQL
              || schema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
            execute(mycat, useSchema, sql, sqlContext, sqlType);
            return;
          }
        }
        default: {
          IGNORED_SQL_LOGGER.warn("ignore:{}", sql);
          mycat.writeOkEndPacket();
        }
      }
    } catch (Exception e) {
      mycat.setLastMessage(e);
      mycat.writeErrorEndPacket();
    }
  }

  private void setCharset(MycatSession mycat, BufferSQLContext sqlContext) {
    String charset = sqlContext.getCharset();
    mycat.setCharset(charset);
    mycat.writeOkEndPacket();
    return;
  }

  private void useSchema(MycatSession mycat, BufferSQLContext sqlContext) {
    String schemaName = sqlContext.getSchemaName(0);
    useSchema(mycat, schemaName);
    return;
  }

  private void setAutocommit(MycatSession mycat, BufferSQLContext sqlContext) {
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

  private void setTranscation(MycatSession mycat, String sql, BufferSQLContext sqlContext) {
    if (sqlContext.isAccessMode()) {
      mycat.setAccessModeReadOnly(true);
      mycat.writeOkEndPacket();
      return;
    }
    if (sqlContext.getTransactionLevel() == MySQLIsolationLevel.GLOBAL) {
      IGNORED_SQL_LOGGER.warn("unsupport global send error", sql);
      mycat.setLastMessage("unsupport global level");
      mycat.writeErrorEndPacket();
      return;
    }
    MySQLIsolation isolation = sqlContext.getIsolation();
    if (isolation == null) {
      mycat.setLastMessage("set manager fail!");
      mycat.writeErrorEndPacket();
      return;
    }
    mycat.setIsolation(isolation);
    mycat.writeOkEndPacket();
    return;
  }


  public void execute(MycatSession mycat, MycatSchema useSchema, String sql,
      BufferSQLContext sqlContext, byte sqlType) {
    boolean simpleSelect = sqlContext.isSimpleSelect() && sqlType == SELECT_SQL;
    if (useSchema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
      MySQLDataSourceQuery query = new MySQLDataSourceQuery();
      query.setRunOnMaster(!simpleSelect);
      MySQLTaskUtil.proxyBackend(mycat, sql, useSchema.getDefaultDataNode(), query);
      return;
    }
    ProxyRouteResult resultRoute = router.enterRoute(useSchema, sqlContext, sql);
    if (resultRoute == null) {
      mycat.setLastMessage("can not route:" + sql);
      mycat.writeErrorEndPacket();
      return;
    }
    MySQLDataSourceQuery query = new MySQLDataSourceQuery();
    query.setIds(null);
    query.setRunOnMaster(resultRoute.isRunOnMaster(!simpleSelect));
    query.setStrategy(PlugRuntime.INSTCANE
        .getLoadBalanceByBalanceName(resultRoute.getBalance()));
    MySQLTaskUtil
        .proxyBackend(mycat, resultRoute.getSql(), resultRoute.getDataNode(), query);
  }



  public void useSchema(MycatSession mycat, String schemaName) {
    mycat.useSchema(schemaName);
    mycat.writeOkEndPacket();
  }
}
