//package io.mycat.lib;
//
//import io.mycat.calcite.BackendTableInfo;
//import io.mycat.calcite.MetadataManager;
//import io.mycat.pattern.InstructionSet;
//
//public class ShardingQueryExport implements InstructionSet {
//
//    public final static BackendTableInfo getPartitionInformation(String schemaName, String tableName, String partitionValue){
//        BackendTableInfo backEndTableInfo = MetadataManager.INSATNCE.getNatrueBackEndTableInfo(schemaName, tableName, partitionValue);
//        return backEndTableInfo;
//    }
//}