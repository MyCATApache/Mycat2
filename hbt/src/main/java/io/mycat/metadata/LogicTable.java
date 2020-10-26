package io.mycat.metadata;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.DataNode;
import io.mycat.LogicTableType;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.hbt3.CustomTable;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.router.CustomRuleFunction;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

@Getter
public class LogicTable {
    private final String uniqueName;
    private final LogicTableType type;
    private final String schemaName;
    private final String tableName;
    private final List<SimpleColumnInfo> rawColumns;
    private final String createTableSQL;
    private final SimpleColumnInfo autoIncrementColumn;

    //优化,非必须
    private final Map<String, SimpleColumnInfo> map;


    public LogicTable(LogicTableType type, String schemaName,
                      String tableName,
                      List<SimpleColumnInfo> rawColumns,
                      String createTableSQL) {
        /////////////////////////////////////////
        this.uniqueName = schemaName + "_" + tableName;
        this.type = type;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.rawColumns = rawColumns;
        SQLStatement createTableAst = SQLUtils.parseSingleMysqlStatement(createTableSQL);
        if (createTableAst instanceof SQLCreateTableStatement ){
            ((SQLCreateTableStatement) createTableAst).setIfNotExiists(true);
            ((SQLCreateTableStatement ) createTableAst).setSchema(schemaName);
        }
        if (createTableAst instanceof MySqlCreateTableStatement){
            ((MySqlCreateTableStatement) createTableAst).setIfNotExiists(true);
            ((MySqlCreateTableStatement) createTableAst).setSchema(schemaName);
        }
        if (createTableAst instanceof SQLCreateViewStatement){
            ((SQLCreateViewStatement) createTableAst).setIfNotExists(true);
            SQLExprTableSource tableSource = ((SQLCreateViewStatement) createTableAst).getTableSource();
            tableSource.setSchema(schemaName);
        }
        this.createTableSQL = Objects.requireNonNull(SQLUtils.toMySqlString(createTableAst), this.uniqueName + " createTableSQL is not existed");
        /////////////////////////////////////////
        this.autoIncrementColumn = rawColumns.stream().filter(i -> i.isAutoIncrement()).findFirst().orElse(null);
        /////////////////////////////////////////
        Map<String, SimpleColumnInfo> result = new HashMap<>();
        for (SimpleColumnInfo k : rawColumns) {
            result.put(k.getColumnName(), k);
        }
        this.map = result;
    }

    public static TableHandler createGlobalTable(String schemaName,
                                                 String tableName,
                                                 List<DataNode> backendTableInfos,
                                                 LoadBalanceStrategy loadBalance,
                                                 List<SimpleColumnInfo> columns,
                                                 String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.GLOBAL, schemaName, tableName, columns, createTableSQL);
        return new GlobalTable(logicTable, backendTableInfos, loadBalance);
    }

    public static TableHandler createNormalTable(String schemaName,
                                                 String tableName,
                                                 DataNode dataNode,
                                                 List<SimpleColumnInfo> columns,
                                                 String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.NORMAL, schemaName, tableName, columns, createTableSQL);
        return new NormalTable(logicTable, dataNode);
    }

    public static ShardingTable createShardingTable(String schemaName,
                                                    String tableName,
                                                    List<DataNode> backendTableInfos,
                                                    List<SimpleColumnInfo> columns,
                                                    CustomRuleFunction function,
                                                    Supplier<String> sequence,
                                                    String createTableSQL) {
        LogicTable logicTable = new LogicTable(LogicTableType.SHARDING, schemaName, tableName, columns, createTableSQL);
        return new ShardingTable(logicTable, backendTableInfos, function, sequence);
    }

    public static TableHandler createCustomTable(CustomTable o) {
        return null;
    }

    public SimpleColumnInfo getColumnByName(String name) {
        SimpleColumnInfo simpleColumnInfo = this.map.get(name);
        if (simpleColumnInfo == null) {
            SimpleColumnInfo simpleColumnInfo1 = this.map.get(name.toLowerCase());
            if (simpleColumnInfo1 == null) {
                return this.map.get(name.toUpperCase());
            } else {
                return simpleColumnInfo1;
            }
        } else {
            return simpleColumnInfo;
        }
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public static String rewriteCreateTableSql(String sql,String schemaName, String tableName) {
        SQLStatement createTableAst = SQLUtils.parseSingleMysqlStatement(sql);
        if (createTableAst instanceof SQLCreateTableStatement){
            SQLCreateTableStatement tableStatement = (SQLCreateTableStatement) createTableAst;
            tableStatement.setTableName(tableName);
            tableStatement.setSchema(schemaName);
        }
        if (createTableAst instanceof MySqlCreateTableStatement){
            MySqlCreateTableStatement tableStatement = (MySqlCreateTableStatement) createTableAst;
            tableStatement.setTableName(tableName);
            tableStatement.setSchema(schemaName);
        }
        if (createTableAst instanceof SQLCreateViewStatement){
            SQLExprTableSource tableSource = ((SQLCreateViewStatement) createTableAst).getTableSource();
            tableSource.setSimpleName(tableName);
            tableSource.setSchema(schemaName);
        }
        return createTableAst.toString();
    }
}
