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
package io.mycat.router;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.config.schema.SchemaType;
import io.mycat.logTip.RouteNullChecker;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.routeStrategy.SqlParseRouteRouteStrategy;
import io.mycat.sqlparser.util.BufferSQLContext;
import io.mycat.sqlparser.util.BufferSQLParser;

/**
 * @author jamie12221 date 2019-05-05 17:04
 **/
public class MycatRouter implements RouteStrategy<RouteContext> {

  final MycatRouterConfig config;
  final RouteContext context;
  final BufferSQLContext sqlContext;

  private BufferSQLParser sqlParser() {
    return new BufferSQLParser();
  }

  public MycatRouter(MycatRouterConfig config) {
    this.config = config;
    this.context = new RouteContext(config);
    this.sqlContext = new BufferSQLContext();
  }

  private BufferSQLContext sqlContext() {
    return sqlContext;
  }

  public ResultRoute enterRoute(MycatSchema defaultSchema, String sql) {
    BufferSQLContext bufferSQLContext = sqlContext();
    sqlParser().parse(sql.getBytes(), bufferSQLContext);
    return enterRoute(defaultSchema, bufferSQLContext, sql);
  }

  public BufferSQLContext simpleParse(String sql) {
    BufferSQLContext bufferSQLContext = sqlContext();
    sqlParser().parse(sql.getBytes(), bufferSQLContext);
    return bufferSQLContext;
  }

  public ResultRoute enterRoute(MycatSchema defaultSchema, BufferSQLContext sqlContext,
      String sql) {
    this.context.clear();
    this.context.setSqlContext(sqlContext);
    int sqlType = sqlContext.getSQLType();

    MycatProxyStaticAnnotation sa = sqlContext.getStaticAnnotation()
        .toMapAndClear(this.context.getStaticAnnotation());
    String balance = sa.getBalance();
    Boolean runOnMaster = sa.getRunOnMaster();

    //判断有没有schema
    if (sa.getSchema() != null) {
      defaultSchema = config.getSchemaBySchemaName(sa.getSchema());
      if (defaultSchema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
        OneServerResultRoute routeResult = new OneServerResultRoute();
        routeResult.setDataNode(defaultSchema.getDefaultDataNode());
        routeResult.setSql(sql);
        return routeResult
            .setBalance(balance).setRunOnMaster(runOnMaster);
      }
    }

    int schemaCount = sqlContext.getSchemaCount();
    if (schemaCount == 0) {
      RouteStrategy routeStrategy = defaultSchema.getRouteStrategy();
      return routeStrategy.route(defaultSchema, sql, this.context)
          .setBalance(balance).setRunOnMaster(runOnMaster)
          ;
    }
    if (schemaCount == 1) {
      String schemaName = sqlContext.getSchemaName(0);
      MycatSchema schema = config.getSchemaBySchemaName(schemaName);
      RouteNullChecker.CHECK_MYCAT_SCHEMA_EXIST.check(schemaName, schema != null);
      RouteStrategy routeStrategy = schema.getRouteStrategy();
      return routeStrategy.route(schema, sql, this.context)
          .setBalance(balance).setRunOnMaster(runOnMaster)
          ;
    } else {

      return this.route(defaultSchema, sql, this.context)
          .setBalance(balance).setRunOnMaster(runOnMaster);
    }
  }
//
//  public MySQLCommandRouteResultRoute enterRoute(String defaultSchemaName, int commandPakcet) {
//    MycatSchema defaultSchema = config.getSchemaBySchemaName(defaultSchemaName);
//    return enterRoute(defaultSchema, commandPakcet);
//  }
//
//  public MySQLCommandRouteResultRoute enterRoute(MycatSchema defaultSchema, int commandPakcet) {
//    String defaultDataNode = defaultSchema.getDefaultDataNode();
//    MySQLCommandRouteResultRoute result = new MySQLCommandRouteResultRoute();
//    result.setCmd(commandPakcet);
//    result.setDataNode(defaultDataNode);
//    return result;
//  }

  public ResultRoute enterRoute(String defaultSchemaName, String sql) {
    MycatSchema defaultSchema = config.getSchemaBySchemaName(defaultSchemaName);
    RouteNullChecker.CHECK_MYCAT_SCHEMA_EXIST.check(defaultSchemaName, defaultSchema != null);
    return enterRoute(defaultSchema, sql);
  }

  @Override
  public ResultRoute route(MycatSchema schema, String sql, RouteContext routeContext) {
    SqlParseRouteRouteStrategy strategy = routeContext.getSqlParseRouteRouteStrategy();
    strategy.route(schema, sql, context);
    return null;
  }

  public MycatRouterConfig getConfig() {
    return config;
  }

  public MycatSchema getDefaultSchema() {
    return config.getDefaultSchema();
  }

  public MycatSchema getSchemaBySchemaName(String db) {
    return config.getSchemaBySchemaName(db);
  }
}
