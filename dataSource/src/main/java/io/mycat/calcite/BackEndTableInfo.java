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
    private String hostname;
    private String schemaName;
    private String tableName;
}
