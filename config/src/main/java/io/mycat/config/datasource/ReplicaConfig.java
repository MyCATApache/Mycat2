/**
 * Copyright (C) <2019>  <chen junwen,gaozhiwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


package io.mycat.config.datasource;


import io.mycat.config.GlobalConfig;

import java.util.List;

/**
 * Desc: 数据源replica配置类
 *
 * @date: 24/09/2017
 * @author: gaozhiwen
 */
public class ReplicaConfig {
    public enum BalanceTypeEnum {
        BALANCE_ALL, BALANCE_ALL_READ, BALANCE_NONE
    }

    public enum RepSwitchTypeEnum {
        NOT_SWITCH, SWITCH
    }

    public enum RepTypeEnum {
        // 单一节点
        SINGLE_NODE(GlobalConfig.SINGLE_NODE_HEARTBEAT_SQL, GlobalConfig.MYSQL_SLAVE_STAUTS_COLMS),
        // 普通主从
        MASTER_SLAVE(GlobalConfig.MASTER_SLAVE_HEARTBEAT_SQL, GlobalConfig.MYSQL_SLAVE_STAUTS_COLMS),
        // 普通基于garela cluster集群
        GARELA_CLUSTER(GlobalConfig.GARELA_CLUSTER_HEARTBEAT_SQL, GlobalConfig.MYSQL_CLUSTER_STAUTS_COLMS);

        private byte[] hearbeatSQL;
        private String[] fetchColms;

        RepTypeEnum(String hearbeatSQL, String[] fetchColms) {
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

    private String name;
    private RepTypeEnum repType;
    private RepSwitchTypeEnum switchType;
    private BalanceTypeEnum balanceType;
    private List<DatasourceConfig> mysqls;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RepTypeEnum getRepType() {
        return repType;
    }

    public void setRepType(RepTypeEnum repType) {
        this.repType = repType;
    }

    public RepSwitchTypeEnum getSwitchType() {
        return switchType;
    }

    public void setSwitchType(RepSwitchTypeEnum switchType) {
        this.switchType = switchType;
    }

    public BalanceTypeEnum getBalanceType() {
        return balanceType;
    }

    public void setBalanceType(BalanceTypeEnum balanceType) {
        this.balanceType = balanceType;
    }

    public List<DatasourceConfig> getMysqls() {
        return mysqls;
    }

    public void setMysqls(List<DatasourceConfig> mysqls) {
        this.mysqls = mysqls;
    }

    @Override
    public String toString() {
        return "ReplicaConfig{" + "name='" + name + '\'' + ", repType=" + repType + ", switchType=" + switchType + ", balanceType=" + balanceType
                + ", mysqls=" + mysqls + '}';
    }
}
