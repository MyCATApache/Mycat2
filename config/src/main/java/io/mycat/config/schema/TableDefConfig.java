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

import io.mycat.config.route.DynamicAnnotationConfig;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc:
 *
 * @date: 24/09/2017 2/05/2019
 * @author: gaozhiwen chenjunwen
 */
public class TableDefConfig {

  public enum MycatTableType {
     GLOBAL, SHARING_DATABASE, SHARING_TABLE, SHARING_DATABASE_TABLE, ER;
  }

  private String name;
  private MycatTableType tableType;
  private DynamicAnnotationConfig dynamicAnnotation;
  private List<String> shardingKeys;
  private List<String> shardingRule;
  private String dataNode;
  private List<String> subTables = new ArrayList<>();
  private String sharingRule;
  private String primaryKey;
  private boolean autoIncrement;
  private boolean defaultLimit;
  private List<String> dataNodes = new ArrayList<String>();
  private List<ERChildTableConfig> childTableConfigs = new ArrayList<>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public String getDataNode() {
    return dataNode;
  }

  public void setDataNode(String dataNode) {
    this.dataNode = dataNode;
  }


  public List<String> getDataNodes() {
    return dataNodes;
  }

  public void setDataNodes(List<String> dataNodes) {
    this.dataNodes = dataNodes;
  }

  @Override
  public String toString() {
    return "TableDefConfig{" +
               "name='" + name + '\'' +
               ", tableType=" + tableType +
               ", dynamicAnnotation=" + dynamicAnnotation +
               ", shardingKeys=" + shardingKeys +
               ", shardingRule=" + shardingRule +
               ", dataNode='" + dataNode + '\'' +
               ", subTables=" + subTables +
               ", sharingRule='" + sharingRule + '\'' +
               ", primaryKey='" + primaryKey + '\'' +
               ", autoIncrement=" + autoIncrement +
               ", defaultLimit=" + defaultLimit +
               ", dataNodes=" + dataNodes +
               ", childTableConfigs=" + childTableConfigs +
               '}';
  }
}
