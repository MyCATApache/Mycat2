//package io.mycat.metadata;
//
//import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
//import io.mycat.MetaClusterCurrent;
//import io.mycat.MetadataStorageManager;
//import io.mycat.Ops;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Map;
//
//public class DDLOps implements AutoCloseable {
//    private static final Logger LOGGER = LoggerFactory.getLogger(DDLOps.class);
//    private final MetadataStorageManager metadataStorageManager;
//    private final Ops ops;
//    private MetadataManager metadataManager;
//
//    public DDLOps(MetadataManager metadataManager) {
//        this.metadataManager = metadataManager;
//        this.metadataStorageManager = MetaClusterCurrent.wrapper(MetadataStorageManager.class);
//        this.ops = this.metadataStorageManager.startOps();
//        metadataManager.
//    }
//
//    @Override
//    public void close() {
//        ops.close();
//    }
//
//    public void commit() {
//        LOGGER.info("commit");
//        ops.commit();
//    }
//
//    public void addSchema(String name, String targetName) {
//        LOGGER.info(" addSchema :{},{}", name, targetName);
//        ops.addSchema( name, targetName);
//    }
//
//    public void addTargetOnExistedSchema(String name, String targetName) {
//        LOGGER.info(" addTargetOnExistedSchema :{},{}", name, targetName);
//        ops.addTargetOnExistedSchema(name,targetName);
//    }
//
//    public void dropSchema(String schemaName) {
//        LOGGER.info(" dropSchema :{}", schemaName);
//        ops.dropSchema(schemaName);
//    }
//
//    public void createNormalTable(String schemaName,
//                                  String tableName,
//                                  MySqlCreateTableStatement sqlString) {
//        LOGGER.info(" createNormalTable :{}.{} sql:{}", schemaName, tableName, sqlString);
//        ops.putNormalTable(schemaName,tableName,sqlString);
//    }
//
//    public void createNormalTable(String schemaName,
//                                  String tableName,
//                                  MySqlCreateTableStatement sqlString,
//                                  String targetName) {
//        LOGGER.info(" createNormalTable :{}.{} sql:{} targetName:{}", schemaName, tableName, sqlString, targetName);
//        ops.putNormalTable(schemaName,tableName,sqlString,targetName);
//    }
//
//    public void createGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement ast) {
//        LOGGER.info(" createGlobalTable :{}.{} sql:{}", schemaName, tableName, ast);
//        ops.putGlobalTable(schemaName,tableName,ast);
//    }
//
//    public void createShardingTable(String schemaName,
//                                    String tableName,
//                                    MySqlCreateTableStatement tableStatement,
//                                    Map<String, Object> infos) {
//        LOGGER.info(" createShardingTable :{}.{} sql:{} targetName:{}", schemaName, tableName, tableStatement, infos);
//        ops.putShardingTable(schemaName, tableName,tableStatement, infos);
//    }
//}
