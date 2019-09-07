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
