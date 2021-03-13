package io.mycat.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorManager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.mycat.util.DDLHelper.createDatabaseIfNotExist;
import static io.mycat.calcite.table.LogicTable.rewriteCreateTableSql;

public class CreateTableUtils {

    public static void createPhysicalTable(JdbcConnectionManager jdbcConnectionManager, DataNode node,String createSQL) {
        ReplicaSelectorManager selectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
        Set<String> set = new HashSet<>();
        if (selectorRuntime.isDatasource(node.getTargetName())) {
            set.add(node.getTargetName());
        }
        if (selectorRuntime.isReplicaName(node.getTargetName())) {
            set.addAll(selectorRuntime.getReplicaMap().get(node.getTargetName()).getRawDataSourceMap().keySet());
        }
        if (set.isEmpty()){
            throw new IllegalArgumentException();
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

        // 删掉阿里的 全局表语法 (不使用)
        List<SQLTableElement> tableElementList = mySqlCreateTableStatement.getTableElementList();
        if(tableElementList != null){
            tableElementList.removeIf(e-> e instanceof MySqlTableIndex && ((MySqlTableIndex) e).isGlobal());
        }
        return mySqlCreateTableStatement.toString();
    }

}
