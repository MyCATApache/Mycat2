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


import io.mycat.calcite.shardingQuery.SchemaInfo;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
@Data
@EqualsAndHashCode
@Builder
public class BackendTableInfo {
    private String replicaName;
    private String datasourceName;
    private SchemaInfo schemaInfo;

    public BackendTableInfo() {
    }

    public BackendTableInfo(String replicaName, String hostName, SchemaInfo schemaInfo) {
        this.replicaName = replicaName;
        this.datasourceName = hostName;
        this.schemaInfo = schemaInfo;
    }


//
//    public <T> T getSession(boolean runOnMaster, LoadBalanceStrategy balanceStrategy) {
//        JdbcDataSource datasource = getDatasource(runOnMaster, balanceStrategy);
//        return (T) datasource.getReplica().getDefaultConnection(datasource);
//    }

    //    String getDatasourceName(boolean runOnMaster, LoadBalanceStrategy balanceStrategy) {
//        return getDatasource(runOnMaster, balanceStrategy).getName();
//    }
//
    public String getDatasourceName(LoadBalanceStrategy balanceStrategy) {
        if (datasourceName != null) {
            return datasourceName;
        }
        if (replicaName != null) {
            return ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, balanceStrategy).getName();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public String getDatasourceName() {
        return getDatasourceName(null);
    }
}
