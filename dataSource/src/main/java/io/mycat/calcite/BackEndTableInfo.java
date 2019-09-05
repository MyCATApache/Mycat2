package io.mycat.calcite;


import lombok.Builder;
import lombok.Data;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
@Data
@Builder
public class BackEndTableInfo {
    private String hostName;
    private String schemaName;
    private String tableName;
}
