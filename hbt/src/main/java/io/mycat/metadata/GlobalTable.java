package io.mycat.metadata;

import io.mycat.DataNode;
import io.mycat.LogicTableType;
import io.mycat.MetaClusterCurrent;
import io.mycat.SimpleColumnInfo;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;

import java.util.List;
import java.util.function.Supplier;

import static io.mycat.metadata.CreateTableUtils.createPhysicalTable;
import static io.mycat.metadata.CreateTableUtils.normalizeCreateTableSQLToMySQL;

public class GlobalTable implements GlobalTableHandler {
    private final LogicTable logicTable;
    private final List<DataNode> backendTableInfos;


    public GlobalTable(LogicTable logicTable,
                       List<DataNode> backendTableInfos) {
        this.logicTable = logicTable;
        this.backendTableInfos = backendTableInfos;
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
    public Supplier<Number> nextSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            DDLHelper.createDatabaseIfNotExist(connection, getSchemaName());
            connection.executeUpdate(normalizeCreateTableSQLToMySQL(getCreateTableSQL()), false);
        }catch (Exception e){
            e.printStackTrace();
        }
        for (DataNode node : getGlobalDataNode()) {
            createPhysicalTable(jdbcConnectionManager,node,getCreateTableSQL());
        }
    }


    @Override
    public void dropPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        String dropTemplate = "drop table `%s`.`%s`";
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(String.format(dropTemplate, getSchemaName(), getTableName()), false);
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