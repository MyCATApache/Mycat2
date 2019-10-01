package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.calcite.BackEndTableInfo;
import io.mycat.calcite.MetadataManager;

public class ShardingQueryExport implements InstructionSet {

    public final static BackEndTableInfo getPartitionInformation(String schemaName, String tableName, String partitionValue){
        BackEndTableInfo backEndTableInfo = MetadataManager.INSATNCE.getBackEndTableInfo(schemaName, tableName, partitionValue);
        return backEndTableInfo;
    }
}