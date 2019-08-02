package io.mycat.calcite;


import lombok.Data;

@Data
public class BackEndTableInfo {

    public String hostname;
    public String schemaName;
    public String tableName;
    public BackEndTableInfo(String hostname, String schemaName, String tableName) {
        this.hostname = hostname;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }
}
