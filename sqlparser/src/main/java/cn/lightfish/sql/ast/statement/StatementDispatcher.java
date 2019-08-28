package cn.lightfish.sql.ast.statement;

import cn.lightfish.sql.ast.SQLTypeMap;
import cn.lightfish.sql.ast.complier.ComplierContext;
import cn.lightfish.sql.ast.complier.ExprComplier;
import cn.lightfish.sql.ast.converter.Converters;
import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.persistent.InsertPersistent;
import cn.lightfish.sql.persistent.PersistentManager;
import cn.lightfish.sql.persistent.UpdatePersistent;
import cn.lightfish.sql.schema.*;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.*;

public class StatementDispatcher extends MySqlASTVisitorAdapter {

    final MycatConsole console;
    Executor consoleResult;

    public StatementDispatcher(MycatConsole console) {
        this.console = console;
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        consoleResult = console.getComplierContext().getRootQueryComplier().complieRootQuery(x);
        return super.visit(x);
    }


    @Override
    public boolean visit(MySqlInsertStatement x) {
        ComplierContext complierContext = console.getComplierContext();
        ExprComplier exprComplier = complierContext.getExprComplier();
        MycatTable table = console.getCurrentSchema()
                .getTableByName(x.getTableSource().getSchemaObject().getName());
        List<SQLExpr> columns = x.getColumns();
        int count = columns.size();
        TableColumnDefinition[] columnNameList = new TableColumnDefinition[count];
        for (int i = 0; i < count; i++) {
            columnNameList[i] = table.getColumnByName(Converters.getColumnName((SQLName) columns.get(i)));
        }
        Map<String, Object> persistentAttribute = new HashMap<>();
        InsertPersistent insertPersistent = PersistentManager.INSTANCE
                .getInsertPersistent(console, table, columnNameList, persistentAttribute);
        List<ValuesClause> valuesList = x.getValuesList();
        for (ValuesClause valuesClause : valuesList) {
            List<SQLExpr> values = valuesClause.getValues();
            Object[] row = new Object[count];
            for (int i = 0; i < count; i++) {
                SQLExpr valueExpr = values.get(i);
                ValueExpr expr = exprComplier.createExpr(valueExpr);
                row[i] = expr.getValue();
            }
            insertPersistent.insert(row);
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLCreateDatabaseStatement x) {
        console.createSchema(x.getDatabaseName());
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLShowDatabasesStatement x) {
        console.showDatabase();
        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlCreateTableStatement x) {
        MycatSchema currnetSchema = console.getCurrentSchema();
        Objects.requireNonNull(currnetSchema);
        String tableName = x.getTableSource().getSchemaObject().getName();
        List<SQLTableElement> tableElementList =
                x.getTableElementList() == null ? Collections.emptyList() : x.getTableElementList();
        List<TableColumnDefinition> columnDefinitions = new ArrayList<>(tableElementList.size());
        String primaryKey = null;
        for (SQLTableElement sqlTableElement : tableElementList) {
            if (sqlTableElement instanceof SQLColumnDefinition) {
                SQLColumnDefinition columnDefinition = (SQLColumnDefinition) sqlTableElement;
                String columnName = columnDefinition.getColumnName();
                SQLDataType dataType = columnDefinition.getDataType();
                TableColumnDefinition mycatColumnDefinition = new TableColumnDefinition(columnName,
                        SQLTypeMap.toClass(dataType.jdbcType()));
                columnDefinitions.add(mycatColumnDefinition);
            } else if (sqlTableElement instanceof SQLColumnPrimaryKey) {
                SQLColumnPrimaryKey columnPrimaryKey = (SQLColumnPrimaryKey) sqlTableElement;
                primaryKey = columnPrimaryKey.getName().getSimpleName();
            }
        }
        SQLMethodInvokeExpr dbPartitionBy = (SQLMethodInvokeExpr) x
                .getDbPartitionBy();//指定分库键和分库算法，不支持按照时间分库；
        if (dbPartitionBy != null) {
            console.createTable(new MycatTable(currnetSchema, tableName, columnDefinitions,
                    getMycatPartition(x, primaryKey, dbPartitionBy)));
        } else {
            console.createTable(
                    new MycatTable(currnetSchema, tableName, columnDefinitions, x.isBroadCast()));
        }
        return super.visit(x);
    }

    private MycatPartition getMycatPartition(MySqlCreateTableStatement x, String primaryKey,
                                             SQLMethodInvokeExpr dbPartitionBy) {
        String dbMethodName = dbPartitionBy.getMethodName();//分片算法名字
        String dbPartitionCoulumn = dbPartitionBy.getArguments() == null ? primaryKey
                : dbPartitionBy.getArguments().get(0).toString();
        SQLMethodInvokeExpr tablePartitionBy = (SQLMethodInvokeExpr) x
                .getTablePartitionBy();//默认与 DBPARTITION BY 相同，指定物理表使用什么方式映射数据；
        String tableMethodName;
        String tablePartitionCoulumn;
        int tablePartitions;
        if (tablePartitionBy != null) {
            tableMethodName = tablePartitionBy.getMethodName();//分片算法名字
            tablePartitionCoulumn = tablePartitionBy.getArguments() == null ? primaryKey
                    : tablePartitionBy.getArguments().get(0).toString();
            tablePartitions = (x.getTablePartitions() == null ? 1
                    : ((SQLIntegerExpr) x.getTablePartitions()).getNumber().intValue());
        } else {
            tableMethodName = dbMethodName;
            tablePartitionCoulumn = dbPartitionCoulumn;
            tablePartitions = 1;
        }
        return new MycatPartition(dbMethodName, dbPartitionCoulumn, tableMethodName, tablePartitionCoulumn, tablePartitions);
    }

    @Override
    public boolean visit(SQLDropDatabaseStatement x) {
        console.dropDatabase(x.getDatabaseName());
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLDropTableGroupStatement x) {
        console.dropTable(x.getTableGroupName());
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLDropTableStatement x) {
        List<SQLExprTableSource> tableSources = x.getTableSources();
        List<String> nameList = new ArrayList<>();
        for (SQLExprTableSource tableSource : tableSources) {
            nameList.add(tableSource.getTableName());
        }
        console.dropTable(nameList);

        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        ComplierContext complierContext = console.getComplierContext();
        ExprComplier exprComplier = complierContext.getExprComplier();
        SQLExprTableSource tableSource = (SQLExprTableSource) x.getTableSource();
        MycatTable table = console.getCurrentSchema()
                .getTableByName(tableSource.getSchemaObject().getName());
        complierContext.createColumnAllocator(x);
        ValueExpr expr = exprComplier.createExpr(x.getWhere());
        TableColumnDefinition[] columnDefinition = complierContext.getColumnAllocatior()
                .getLeafTableColumnDefinition(tableSource);
        UpdatePersistent updatePersistent = PersistentManager.INSTANCE
                .getUpdatePersistent(console, table, columnDefinition, Collections.emptyMap());

        return super.visit(x);
    }

    public Executor getConsoleResult() {
        return consoleResult;
    }
}