package io.mycat.metadata;

import io.mycat.*;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static io.mycat.metadata.DDLHelper.createDatabaseIfNotExist;
import static io.mycat.metadata.LogicTable.rewriteCreateTableSql;

public class NormalTable implements NormalTableHandler {
    private final GlobalTable table;

    public NormalTable(LogicTable logicTable, DataNode backendTable) {
        this.table = new GlobalTable(logicTable, Collections.singletonList(backendTable), null);
    }

    @Override
    public DataNode getDataNode() {
        return this.table.getGlobalDataNode().get(0);
    }

//    @Override
//    public Function<MySqlInsertStatement, Iterable<TextUpdateInfo>> insertHandler() {
//        return this.table.insertHandler();
//    }

//    @Override
//    public Function<MySqlUpdateStatement, Iterable<TextUpdateInfo>> updateHandler() {
//        return this.table.updateHandler();
//    }

//    @Override
//    public Function<MySqlDeleteStatement, Iterable<TextUpdateInfo>> deleteHandler() {
//        return this.table.deleteHandler();
//    }

    @Override
    public LogicTableType getType() {
        return LogicTableType.NORMAL;
    }

    @Override
    public String getSchemaName() {
        return this.table.getSchemaName();
    }

    @Override
    public String getTableName() {
        return this.table.getTableName();
    }

    @Override
    public String getCreateTableSQL() {
        return this.table.getCreateTableSQL();
    }

    @Override
    public List<SimpleColumnInfo> getColumns() {
        return this.table.getColumns();
    }

    @Override
    public SimpleColumnInfo getColumnByName(String name) {
        return this.table.getColumnByName(name);
    }

    @Override
    public SimpleColumnInfo getAutoIncrementColumn() {
        return this.table.getAutoIncrementColumn();
    }

    @Override
    public String getUniqueName() {
        return this.table.getUniqueName();
    }

    @Override
    public Supplier<Number> nextSequence() {
        return this.table.nextSequence();
    }

    @Override
    public void createPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            DDLHelper.createDatabaseIfNotExist(connection, getSchemaName());
            connection.executeUpdate(normalizeCreateTableSQLToMySQL(getCreateTableSQL()), false);
        }
        for (DataNode node : Collections.singleton(getDataNode())) {
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(node.getTargetName())) {
                DDLHelper.createDatabaseIfNotExist(connection, node);
                connection.executeUpdate(rewriteCreateTableSql(normalizeCreateTableSQLToMySQL(getCreateTableSQL()), node.getSchema(), node.getTable()), false);
            }
        }
    }


    @Override
    public void dropPhysicalTables() {
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        String dropTemplate = "drop table `%s`.`%s`";
        try (DefaultConnection connection = jdbcConnectionManager.getConnection("prototype")) {
            connection.executeUpdate(String.format(dropTemplate, getSchemaName(), getTableName()), false);
        }
//        for (DataNode node :Collections.singleton(getDataNode())) {
//            try (DefaultConnection connection = jdbcConnectionManager.getConnection(node.getTargetName())) {
//                connection.executeUpdate(String.format(dropTemplate,node.getSchema(),node.getTable()), false);
//            }
//        }
    }
}