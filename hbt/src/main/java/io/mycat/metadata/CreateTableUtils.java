package io.mycat.metadata;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.HashSet;
import java.util.Set;

import static io.mycat.metadata.DDLHelper.createDatabaseIfNotExist;
import static io.mycat.metadata.LogicTable.rewriteCreateTableSql;

public class CreateTableUtils {

    public static void createPhysicalTable(JdbcConnectionManager jdbcConnectionManager, DataNode node,String createSQL) {
        ReplicaSelectorRuntime selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
        Set<String> set = new HashSet<>();
        if (selectorRuntime.isDatasource(node.getTargetName())) {
            set.add(node.getTargetName());
        }
        if (selectorRuntime.isReplicaName(node.getTargetName())) {
            set.addAll(selectorRuntime.getRepliaNameListByInstanceName(node.getTargetName()));
        }
        for (String s : set) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(s)) {
                createDatabaseIfNotExist(connection, node);
                connection.executeUpdate(rewriteCreateTableSql(normalizeCreateTableSQLToMySQL(createSQL), node.getSchema(), node.getTable()), false);
            }
        }
    }
    public static String normalizeCreateTableSQLToMySQL(String createTableSQL) {
        MySqlCreateTableStatement mySqlCreateTableStatement = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSQL);
        mySqlCreateTableStatement.setBroadCast(false);
        mySqlCreateTableStatement.setDbPartitionBy(null);
        mySqlCreateTableStatement.setDbPartitions(null);
        mySqlCreateTableStatement.setTableGroup("");
        mySqlCreateTableStatement.setTablePartitionBy(null);
        mySqlCreateTableStatement.setTablePartitions(null);
        return mySqlCreateTableStatement.toString();
    }

}
