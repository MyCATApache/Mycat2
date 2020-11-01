package io.mycat.metadata;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;

import java.util.*;
import java.util.function.Supplier;

import static io.mycat.metadata.LogicTable.rewriteCreateTableSql;

public class GlobalTable implements GlobalTableHandler {
    private final LogicTable logicTable;
    private final List<DataNode> backendTableInfos;
    private final LoadBalanceStrategy balance;


    public GlobalTable(LogicTable logicTable,
                       List<DataNode> backendTableInfos,
                       LoadBalanceStrategy balance) {
        this.logicTable = logicTable;
        this.backendTableInfos = backendTableInfos;
        this.balance = balance;
    }

//    @Override
//    public Function<MySqlInsertStatement, Iterable<ParameterizedValues>> insertHandler() {
//        return sqlStatement1 -> {
//            SQLExprTableSource tableSource = sqlStatement1.getTableSource();
//            return updateHandler(tableSource, sqlStatement1);
//        };
//    }

//    @NotNull
//    private Iterable<ParameterizedValues> updateHandler(SQLExprTableSource tableSource,  SQLStatement sqlStatement1) {
//        return ()->new Iterator<ParameterizedValues>() {
//            Iterator<DataNode> iterator = backendTableInfos.iterator();
//            @Override
//            public boolean hasNext() {
//                return iterator.hasNext();
//            }
//
//            @Override
//            public ParameterizedValues next() {
//                DataNode next = iterator.next();
//                tableSource.setExpr(next.getTargetSchemaTable());
//                return TextUpdateInfo.create(next.getTargetName(), Collections.singletonList(sqlStatement1.toString()));
//            }
//        };
//    }

//    @Override
//    public Function<MySqlUpdateStatement, Iterable<TextUpdateInfo>> updateHandler() {
//        return sqlStatement1 -> {
//            SQLExprTableSource tableSource = (SQLExprTableSource)sqlStatement1.getTableSource();
//            return updateHandler(tableSource, sqlStatement1);
//        };
//    }

//    @Override
//    public Function<MySqlDeleteStatement, Iterable<TextUpdateInfo>> deleteHandler() {
//        return sqlStatement1 -> {
//            SQLExprTableSource tableSource = (SQLExprTableSource)sqlStatement1.getTableSource();
//            return updateHandler(tableSource, sqlStatement1);
//        };
//    }

    @Override
    public LogicTableType getType() {
        return LogicTableType.GLOBAL;
    }

    @Override
    public String getSchemaName() {
        return logicTable.getSchemaName();
    }

    @Override
    public String getTableName() {
        return logicTable.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return logicTable.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return logicTable.getRawColumns();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return logicTable.getColumnByName(name);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return logicTable.getAutoIncrementColumn();
    }

    @Override
    public String getUniqueName() {
        return logicTable.getUniqueName();
    }

    @Override
    public Supplier<String> nextSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(normalizeCreateTableSQLToMySQL(getCreateTableSQL()), false);
        }
        for (DataNode node : getGlobalDataNode()) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(node.getTargetName())) {
                connection.executeUpdate(rewriteCreateTableSql(normalizeCreateTableSQLToMySQL(getCreateTableSQL()),node.getSchema(), node.getTable()), false);
            }
        }
    }

    @Override
    public void dropPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        String dropTemplate = "drop table `%s`.`%s`";
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(String.format(dropTemplate,getSchemaName(),getTableName()), false);
        }
//        for (DataNode node : getGlobalDataNode()) {
//            try (DefaultConnection connection = jdbcConnectionManager.getConnection(node.getTargetName())) {
//                connection.executeUpdate(String.format(dropTemplate,node.getSchema(),node.getTable()), false);
//            }
//        }
    }

    @Override
    public List<DataNode> getGlobalDataNode() {
        return backendTableInfos;
    }
}