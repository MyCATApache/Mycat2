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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Desc:
 *
 * date: 24/09/2017  02/05/2019
 *
 * @author: gaozhiwen chenjunwen
 */
public class SchemaConfig {

  public String name;
  public SchemaType schemaType;
  private String defaultDataNode;
  private String sqlMaxLimit;
  private List<TableDefConfig> tables = new ArrayList<TableDefConfig>();
  private String sequenceModifierClazz;
  private Map<String, String> sequenceModifierProperties;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public SchemaType getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(SchemaType schemaType) {
    this.schemaType = schemaType;
  }

  public String getDefaultDataNode() {
    return defaultDataNode;
  }

  public void setDefaultDataNode(String defaultDataNode) {
    this.defaultDataNode = defaultDataNode;
  }

  public List<TableDefConfig> getTables() {
    return tables;
  }

  public void setTables(List<TableDefConfig> tables) {
    this.tables = tables;
  }

  public String getSqlMaxLimit() {
    return sqlMaxLimit;
  }

  public void setSqlMaxLimit(String sqlMaxLimit) {
    this.sqlMaxLimit = sqlMaxLimit;
  }


  public String getSequenceModifierClazz() {
    return sequenceModifierClazz;
  }


  public void setSequenceModifierClazz(String sequenceModifierClazz) {
    this.sequenceModifierClazz = sequenceModifierClazz;
  }


  public Map<String, String> getSequenceModifierProperties() {
    return sequenceModifierProperties;
  }


  public void setSequenceModifierProperties(Map<String, String> sequenceModifierProperties) {
    this.sequenceModifierProperties = sequenceModifierProperties;
  }

  @Override
  public String toString() {
    return "SchemaConfig{" +
        "name='" + name + '\'' +
        ", schemaType=" + schemaType +
        ", defaultDataNode='" + defaultDataNode + '\'' +
        ", sqlMaxLimit='" + sqlMaxLimit + '\'' +
        ", tables=" + tables +
        ", sequenceModifierClazz='" + sequenceModifierClazz + '\'' +
        ", sequenceModifierProperties=" + sequenceModifierProperties +
        '}';
  }
}
