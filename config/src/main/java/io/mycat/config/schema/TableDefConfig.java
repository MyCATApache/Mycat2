/**
 * Copyright (C) <2019>  <gaozhiwen>
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

package io.mycat.config.schema;

import java.util.List;

/**
 * Desc:
 *
 * @date: 24/09/2017 2/05/2019
 * @author: gaozhiwen chenjunwen
 */
public class TableDefConfig {

  private String balanceName;

  private String name;
  private String dataNodes;
  private String tableRule;
  private String primaryKey;

  public String getBalanceName() {
    return balanceName;
  }

  public List<String> getSubTables() {
    return subTables;
  }

  public void setSubTables(List<String> subTables) {
    this.subTables = subTables;
  }

  private List<String> subTables;
  private boolean autoIncrement;
  private boolean addDefaultLimit;
  private List<ERChildTableConfig> childTableConfigs;

  public MycatTableType getType() {
    return type;
  }

  public void setType(MycatTableType type) {
    this.type = type;
  }

  private MycatTableType type;
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public String getDataNodes() {
    return dataNodes;
  }




  public void setDataNodes(String dataNode) {
    this.dataNodes = dataNode;
  }

  public String getTableRule() {
    return tableRule;
  }

  public void setTableRule(String tableRule) {
    this.tableRule = tableRule;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  public boolean isAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(boolean autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  public boolean isAddDefaultLimit() {
    return addDefaultLimit;
  }

  public void setAddDefaultLimit(boolean addDefaultLimit) {
    this.addDefaultLimit = addDefaultLimit;
  }

  public List<ERChildTableConfig> getChildTableConfigs() {
    return childTableConfigs;
  }

  public void setBalanceName(String balanceName) {
    this.balanceName = balanceName;
  }

  public enum MycatTableType {
    GLOBAL, SHARING_DATABASE, SHARING_TABLE, SHARING_DATABASE_TABLE, ER
  }

  public void setChildTableConfigs(
      List<ERChildTableConfig> childTableConfigs) {
    this.childTableConfigs = childTableConfigs;
  }

  @Override
  public String toString() {
    return "TableDefConfig{" +
               "name='" + name + '\'' +
               ", dataNodes='" + dataNodes + '\'' +
               ", tableRule='" + tableRule + '\'' +
               ", primaryKey='" + primaryKey + '\'' +
               ", autoIncrement=" + autoIncrement +
               ", addDefaultLimit=" + addDefaultLimit +
               ", dataNodes=" + dataNodes +
               ", childTableConfigs=" + childTableConfigs +
               '}';
  }
}
