/**
 * Copyright (C) <2019>  <chen junwen,gaozhiwen>
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


package io.mycat.config.datasource;


import java.util.List;

/**
 * Desc: 数据源replica配置类
 *
 * date: 24/09/2017
 * @author: gaozhiwen
 */
public class ReplicaConfig {

  private String repType;
  private String switchType;
  private String balanceType;

  private String name;
  private String readBalanceName;
  private String writeBalanceName;

  public String getRepType() {
    return repType;
  }

  public void setRepType(String repType) {
    this.repType = repType;
  }
  private List<DatasourceConfig> datasources;

  public String getSwitchType() {
    return switchType;
  }

  private long slaveThreshold;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setSwitchType(String switchType) {
    this.switchType = switchType;
  }

  public String getBalanceType() {
    return balanceType;
  }

  public void setBalanceType(String balanceType) {
    this.balanceType = balanceType;
  }


  public String getReadBalanceName() {
    return readBalanceName;
  }

  public void setReadBalanceName(String readBalanceName) {
    this.readBalanceName = readBalanceName;
  }

  public String getWriteBalanceName() {
    return writeBalanceName;
  }

  public List<DatasourceConfig> getDatasources() {
    return datasources;
  }

  public void setDatasources(List<DatasourceConfig> datasources) {
    this.datasources = datasources;
  }


  public long getSlaveThreshold() {
    return slaveThreshold;
  }


  public void setSlaveThreshold(long slaveThreshold) {
    this.slaveThreshold = slaveThreshold;
  }

  @Override
  public String toString() {
    return "ReplicaConfig{" +
        "name='" + name + '\'' +
        ", repType=" + repType +
        ", switchType=" + switchType +
        ", readBalanceName='" + readBalanceName + '\'' +
        ", datasources=" + datasources +
        ", balanceType=" + balanceType +
        ", slaveThreshold=" + slaveThreshold +
        '}';
  }
}
