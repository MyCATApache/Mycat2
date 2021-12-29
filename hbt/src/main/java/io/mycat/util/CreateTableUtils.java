/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.Partition;
import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.InstanceType;
import io.mycat.replica.ReplicaSelectorManager;
import jdk.nashorn.internal.runtime.options.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.mycat.util.DDLHelper.createDatabaseIfNotExist;
import static io.mycat.calcite.table.LogicTable.rewriteCreateTableSql;

public class CreateTableUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableUtils.class);

    public static void createPhysicalTable(JdbcConnectionManager jdbcConnectionManager, Partition node, String createSQL) {
        ReplicaSelectorManager selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        Set<String> set = new HashSet<>();
        if (selectorRuntime.isDatasource(node.getTargetName())) {
            set.add(node.getTargetName());
        }
        if (selectorRuntime.isReplicaName(node.getTargetName())) {
            set.addAll(selectorRuntime.getReplicaMap().get(node.getTargetName()).getRawDataSourceMap().keySet());
        }
        if (set.isEmpty()) {
            throw new IllegalArgumentException("can not found "+node.getTargetName());
        }
        normalizeCreateTableSQLToMySQL(createSQL).ifPresent(sql -> {
            for (String s : set) {
                try (DefaultConnection connection = jdbcConnectionManager.getConnection(s)) {
                    if (InstanceType.valueOf(connection.getDataSource().getConfig().getInstanceType()).isWriteType()) {
                        connection.createDatabase(node.getSchema());
                        connection.createTable(rewriteCreateTableSql(sql, node.getSchema(), node.getTable()));
                    }
                }
            }
        });
    }

    public static Optional<String> normalizeCreateTableSQLToMySQL(String createTableSQL) {
        Throwable throwable;
        int length = createTableSQL.length();
        try {
            return innerNormalizeCreateTableSQLToMySQL(createTableSQL);
        } catch (Throwable t) {
            throwable = t;
        }
        LOGGER.error("", throwable);
        for (int i = length - 1; 0 < i; i--) {
            try {
                return innerNormalizeCreateTableSQLToMySQL(createTableSQL.substring(0, i));
            } catch (Throwable e) {
                LOGGER.error("", throwable);
                continue;
            }
        }
        return Optional.empty();

    }

    private static Optional<String> innerNormalizeCreateTableSQLToMySQL(String createTableSQL) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(createTableSQL);
        if (sqlStatement instanceof MySqlCreateTableStatement) {
            MySqlCreateTableStatement mySqlCreateTableStatement = (MySqlCreateTableStatement) sqlStatement;
            mySqlCreateTableStatement.setBroadCast(false);
            mySqlCreateTableStatement.setDbPartitionBy(null);
            mySqlCreateTableStatement.setDbPartitions(null);
            mySqlCreateTableStatement.setTableGroup("");
            mySqlCreateTableStatement.setTablePartitionBy(null);
            mySqlCreateTableStatement.setTablePartitions(null);
            mySqlCreateTableStatement.setIfNotExiists(true);

            // 删掉阿里的 全局表语法 (不使用)
            List<SQLTableElement> tableElementList = mySqlCreateTableStatement.getTableElementList();
            if (tableElementList != null) {
                tableElementList.removeIf(e -> e instanceof MySqlTableIndex && ((MySqlTableIndex) e).isGlobal());
            }
            return Optional.ofNullable(mySqlCreateTableStatement.toString());
        } else {
            return Optional.empty();
        }
    }

}
