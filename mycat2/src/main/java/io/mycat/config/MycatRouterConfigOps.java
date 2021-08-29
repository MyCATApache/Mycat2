/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.config;

import com.alibaba.druid.sql.MycatSQLUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.ConfigOps;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.util.NameMap;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;


public class MycatRouterConfigOps implements AutoCloseable {


    public MycatRouterConfigOps() {

    }
    @NotNull
    private MetadataStorageManagerHelper getMetadataStorageManagerHelper() {
        BaseMetadataStorageManager baseMetadataStorageManager = MetaClusterCurrent.wrapper(BaseMetadataStorageManager.class);
        return new MetadataStorageManagerHelper(baseMetadataStorageManager);
    }



    public synchronized void addSchema(String schemaName, String targetName) {
        getMetadataStorageManagerHelper().addSchema(schemaName,targetName);
    }


    public synchronized void putSchema(LogicSchemaConfig schemaConfig) {
        getMetadataStorageManagerHelper().putSchema(schemaConfig);
    }


    public synchronized void dropSchema(String schemaName) {
        getMetadataStorageManagerHelper().dropSchema(schemaName);
    }


    public synchronized void putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        getMetadataStorageManagerHelper().putNormalTable(schemaName,tableName,sqlString);
    }


    public synchronized NormalTableConfig putNormalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString, String targetName) {
       return getMetadataStorageManagerHelper().putNormalTable(schemaName, tableName, sqlString,targetName);
    }

    public synchronized NormalTableConfig putNormalTable(String schemaName, String tableName, NormalTableConfig normalTableConfig) {
        return  getMetadataStorageManagerHelper().putNormalTable(schemaName, tableName, normalTableConfig);
    }

    public synchronized void putTable(CreateTableConfig createTableConfig) {
        getMetadataStorageManagerHelper().putTable(createTableConfig);
    }

    public synchronized GlobalTableConfig putGlobalTable(String schemaName, String tableName, MySqlCreateTableStatement sqlString) {
        return getMetadataStorageManagerHelper().putGlobalTable(schemaName, tableName, sqlString);
    }

    public synchronized GlobalTableConfig putGlobalTableConfig(String schemaName, String tableName, GlobalTableConfig globalTableConfig) {
        return getMetadataStorageManagerHelper().putGlobalTable(schemaName,tableName,globalTableConfig);
    }


    public synchronized void removeTable(String schemaNameArg, String tableNameArg) {
        getMetadataStorageManagerHelper().removeTable(schemaNameArg, tableNameArg);
    }


    public synchronized ShardingTableConfig putRangeTable(String schemaName, String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        return getMetadataStorageManagerHelper().putRangeTable(schemaName, tableName, tableStatement, infos);
    }

    public synchronized ShardingTableConfig putShardingTable(String schemaName, String tableName, ShardingTableConfig config) {
       return getMetadataStorageManagerHelper().putShardingTable(schemaName, tableName, config);
    }


    public synchronized ShardingTableConfig putHashTable(String schemaName,final String tableName, MySqlCreateTableStatement tableStatement, Map<String, Object> infos) {
        return getMetadataStorageManagerHelper().putHashTable(schemaName, tableName, tableStatement, infos);
    }

    public synchronized void putUser(String username, String password, String ip, String transactionType) {
        UserConfig userConfig = UserConfig.builder()
                .username(username)
                .password(password)
                .ip(ip)
                .transactionType(transactionType)
                .build();
        putUser(userConfig);
    }

    public synchronized void putUser(UserConfig userConfig) {
        getMetadataStorageManagerHelper().putUser(userConfig);
    }


    public synchronized void deleteUser(String username) {
        getMetadataStorageManagerHelper().deleteUser(username);
    }

    public synchronized void putSequence(SequenceConfig sequenceConfig) {
        getMetadataStorageManagerHelper().putSequence(sequenceConfig);
    }


    public synchronized void removeSequenceByName(String name) {
        getMetadataStorageManagerHelper().removeSequenceByName(name);
    }


    public synchronized void putDatasource(DatasourceConfig datasourceConfig) {
        getMetadataStorageManagerHelper().putDatasource(datasourceConfig);
    }

    public synchronized void removeDatasource(String datasourceName) {
        getMetadataStorageManagerHelper().removeDatasource(datasourceName);
    }

    public synchronized void putReplica(ClusterConfig clusterConfig) {
        getMetadataStorageManagerHelper().putReplica(clusterConfig);
    }

    public synchronized void removeReplica(String replicaName) {
        getMetadataStorageManagerHelper().removeReplica(replicaName);
    }

    public synchronized void putSqlCache(SqlCacheConfig currentSqlCacheConfig) {
        getMetadataStorageManagerHelper().putSqlCache(currentSqlCacheConfig);
    }

    public synchronized void removeSqlCache(String cacheName) {
        getMetadataStorageManagerHelper().removeSqlCache(cacheName);
    }


    public void commit() throws Exception {

    }

    public void close() {

    }

    public void reset() {
      getMetadataStorageManagerHelper().reset();
    }
}
