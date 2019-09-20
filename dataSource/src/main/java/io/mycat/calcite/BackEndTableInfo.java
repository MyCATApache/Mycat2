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
package io.mycat.calcite;


import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.DsConnection;
import lombok.Builder;
import lombok.Data;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
@Data
@Builder
public class BackEndTableInfo {
    private String dataNodeName;
    private String replicaName;
    private String hostName;
    private String schemaName;
    private String tableName;

    public <T> T getSession() {
        DsConnection dsConnection;
        if (dataNodeName != null) {
            dsConnection = GRuntime.INSTACNE.getJdbcDatasourceSessionByReplicaName(replicaName);
        } else if (replicaName != null) {
            dsConnection = GRuntime.INSTACNE.getJdbcDatasourceSessionByReplicaName(replicaName);
        } else if (hostName != null) {
            dsConnection = GRuntime.INSTACNE.getJdbcDatasourceSessionByName(hostName);
        }else {
            throw new UnsupportedOperationException();
        }
        return (T)dsConnection;
    }
}
