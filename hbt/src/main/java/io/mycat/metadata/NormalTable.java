package io.mycat.metadata;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

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
    public Supplier<String> nextSequence() {
        return this.table.nextSequence();
    }
}