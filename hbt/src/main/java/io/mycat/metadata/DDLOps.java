package io.mycat.metadata;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.config.ShardingBackEndTableInfoConfig;
import io.mycat.config.ShardingFuntion;
import io.mycat.config.ShardingTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DDLOps implements AutoCloseable{
    private static final Logger LOGGER = LoggerFactory.getLogger(DDLOps .class);
    private MetadataManager metadataManager;

    public DDLOps(MetadataManager metadataManager) {

        this.metadataManager = metadataManager;
    }

    @Override
    public void close() {

    }

    public void commit() {
        LOGGER.info("commit");
    }

    public void addSchema(String name, String targetName) {
        LOGGER.info(" addSchema :{},{}",name,targetName);
    }

    public void addTargetOnExistedSchema(String name, String targetName) {
        LOGGER.info(" addTargetOnExistedSchema :{},{}",name,targetName);
    }

    public void dropSchema(String schemaName) {
        LOGGER.info(" dropSchema :{}",schemaName);
    }
    public void createNormalTable(String schemaName,
                                  String tableName,
                                  MySqlCreateTableStatement sqlString) {
        LOGGER.info(" createNormalTable :{}.{} sql:{}",schemaName,tableName,sqlString);
    }
    public void createNormalTable(String schemaName,
                                  String tableName,
                                  MySqlCreateTableStatement sqlString,
                                  String targetName) {
        LOGGER.info(" createNormalTable :{}.{} sql:{} targetName:{}",schemaName,tableName,sqlString,targetName);
    }

    public void createGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement ast) {
        LOGGER.info(" createGlobalTable :{}.{} sql:{}",schemaName,tableName,ast);
    }

    public void createShardingTable(String schemaName, String tableName, MySqlCreateTableStatement ast, Map<String, Object> infos) {
        LOGGER.info(" createShardingTable :{}.{} sql:{} targetName:{}",schemaName,tableName,ast,infos);
        Map<String, String> properties = (Map)infos.get("properties");
        Map<String, String> ranges =(Map)infos.get("ranges");
        List<Map<String,String>> dataNodes = (List)infos.get("dataNodes");
        ShardingTableConfig.ShardingTableConfigBuilder builder = ShardingTableConfig.builder();
        ShardingTableConfig config = builder
                .createTableSQL(ast.toString())
                .function(ShardingFuntion.builder().properties(properties).ranges(ranges).build())
                .dataNodes(dataNodes.stream().map(i -> ShardingBackEndTableInfoConfig
                        .builder()
                        .schemaName(i.get("schemaName"))
                        .tableName(i.get("tableName"))
                        .targetName(i.get("targetName")).build())
                        .collect(Collectors.toList()))
                .build();


    }
}
