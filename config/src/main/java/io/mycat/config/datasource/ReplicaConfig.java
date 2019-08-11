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


import io.mycat.config.GlobalConfig;
import java.util.List;

/**
 * Desc: 数据源replica配置类
 *
 * date: 24/09/2017
 * @author: gaozhiwen
 */
public class ReplicaConfig {
//    public enum BalanceType {
//        BALANCE_ALL, BALANCE_ALL_READ, BALANCE_NONE
//    }

  private RepType repType;
  private RepSwitchType switchType;
  private BalanceType balanceType;

  private String name;

  public RepType getRepType() {
    return repType;
  }

  public void setRepType(RepType repType) {
    this.repType = repType;
  }

  private String balanceName;
  private List<DatasourceConfig> datasources;

  public RepSwitchType getSwitchType() {
    return switchType;
  }

  private long slaveThreshold;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setSwitchType(RepSwitchType switchType) {
    this.switchType = switchType;
  }

  public BalanceType getBalanceType() {
    return balanceType;
  }

  public void setBalanceType(BalanceType balanceType) {
    this.balanceType = balanceType;
  }

  public enum RepSwitchType {
    NOT_SWITCH, SWITCH
  }

  public String getBalanceName() {
    return balanceName;
  }

  public void setBalanceName(String balanceName) {
    this.balanceName = balanceName;
  }

  public List<DatasourceConfig> getDatasources() {
    return datasources;
  }

  public void setDatasources(List<DatasourceConfig> datasources) {
    this.datasources = datasources;
  }

  public enum RepType {
    // 单一节点
    SINGLE_NODE(GlobalConfig.SINGLE_NODE_HEARTBEAT_SQL, GlobalConfig.MYSQL_SLAVE_STAUTS_COLMS),
    // 普通主从
    MASTER_SLAVE(GlobalConfig.MASTER_SLAVE_HEARTBEAT_SQL, GlobalConfig.MYSQL_SLAVE_STAUTS_COLMS),
    // 普通基于garela cluster集群
    GARELA_CLUSTER(GlobalConfig.GARELA_CLUSTER_HEARTBEAT_SQL,
        GlobalConfig.MYSQL_CLUSTER_STAUTS_COLMS),
    NONE("SELECT 1", null),
    ;

    private byte[] hearbeatSQL;
    private String[] fetchColms;

    RepType(String hearbeatSQL, String[] fetchColms) {
      this.hearbeatSQL = hearbeatSQL.getBytes();
      this.fetchColms = fetchColms;
    }

    public byte[] getHearbeatSQL() {
      return hearbeatSQL;
    }

    public String[] getFetchColms() {
      return fetchColms;
    }
  }

  public enum BalanceType {
    // 单独写节点
    BALANCE_ALL, BALANCE_ALL_READ, BALANCE_NONE
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
        ", balanceName='" + balanceName + '\'' +
        ", datasources=" + datasources +
        ", balanceType=" + balanceType +
        ", slaveThreshold=" + slaveThreshold +
        '}';
  }
}
