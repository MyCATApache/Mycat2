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
package io.mycat.beans.mycat;

import io.mycat.config.schema.SchemaConfig;
import io.mycat.config.schema.SchemaType;
import io.mycat.router.RouteStrategy;
import java.util.Map;

public class MycatSchema {
  final SchemaConfig schemaConfig;
  String defaultDataNode;
  final RouteStrategy routeStrategy;
  private Map<String, MycatTable> mycatTables;
  private long sqlMaxLimit = -1;


  public SchemaType getSchema() {
    return getSchemaConfig().getSchemaType();
  }

  public SchemaConfig getSchemaConfig() {
    return schemaConfig;
  }

  public MycatSchema(SchemaConfig schemaConfig, RouteStrategy routeStrategy) {
    this.schemaConfig = schemaConfig;
    this.routeStrategy = routeStrategy;
  }
  public MycatTable getTableByTableName(String name) {
    return mycatTables.get(name);
  }

  public String getDefaultDataNode() {
    return defaultDataNode;
  }

  public String getSchemaName() {
    return schemaConfig.getName();
  }

  public void setSqlMaxLimit(long parseLong) {
    this.sqlMaxLimit = parseLong;
  }

  public void setDefaultDataNode(String defaultDataNode) {
    this.defaultDataNode = defaultDataNode;
  }

  public void setTables(Map<String, MycatTable> mycatTables) {

    this.mycatTables = mycatTables;
  }

  public RouteStrategy getRouteStrategy() {
    return routeStrategy;
  }

  public Map<String, MycatTable> getMycatTables() {
    return mycatTables;
  }

  public long getSqlMaxLimit() {
    return sqlMaxLimit;
  }

  public boolean existTable(String table){
  return   this.mycatTables.containsKey(table);
  }
}
