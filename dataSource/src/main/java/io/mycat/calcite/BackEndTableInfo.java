/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;


import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcDataSourceQuery;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
@Data
@EqualsAndHashCode
@Builder
public class BackEndTableInfo {
    private String dataNodeName;
    private String replicaName;
    private String hostName;
    private String schemaName;
    private String tableName;
    private String targetSchemaTable;

    public BackEndTableInfo() {
    }

    public BackEndTableInfo(String dataNodeName, String replicaName, String hostName, String schemaName, String tableName, String targetSchemaTable) {
        this.dataNodeName = dataNodeName;
        this.replicaName = replicaName;
        this.hostName = hostName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.targetSchemaTable = targetSchemaTable;
    }

    public BackEndTableInfo(String dataNodeName, String replicaName, String hostName, String schemaName, String tableName) {
        this(dataNodeName,replicaName,hostName,schemaName,tableName,schemaName + "." + tableName);
    }

    public <T> T getSession(boolean runOnMaster, LoadBalanceStrategy balanceStrategy) {
        JdbcDataSource datasource = getDatasource(runOnMaster, balanceStrategy);
        return (T) datasource.getReplica().getDefaultConnection(datasource);
    }

    JdbcDataSource getDatasource(boolean runOnMaster, LoadBalanceStrategy balanceStrategy) {
        JdbcDataSource jdbcDataSource;
        if (dataNodeName != null) {
            jdbcDataSource = GRuntime.INSTACNE.getJdbcDatasourceByDataNodeName(dataNodeName, new JdbcDataSourceQuery().setRunOnMaster(runOnMaster).setStrategy(balanceStrategy));
        } else if (replicaName != null) {
            jdbcDataSource = GRuntime.INSTACNE.getJdbcDatasourceSessionByReplicaName(replicaName);
        } else if (hostName != null) {
            jdbcDataSource = GRuntime.INSTACNE.getJdbcDatasourceByName(hostName);
        } else {
            throw new UnsupportedOperationException();
        }
        return jdbcDataSource;
    }
}
