package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.calcite.BackendTableInfo;
import io.mycat.calcite.MetadataManager;

public class ShardingQueryExport implements InstructionSet {

    public final static BackendTableInfo getPartitionInformation(String schemaName, String tableName, String partitionValue){
        BackendTableInfo backEndTableInfo = MetadataManager.INSATNCE.getBackEndTableInfo(schemaName, tableName, partitionValue);
        return backEndTableInfo;
    }
}