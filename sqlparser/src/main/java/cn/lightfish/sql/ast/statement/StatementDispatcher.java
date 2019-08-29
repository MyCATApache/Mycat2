package cn.lightfish.sql.ast.statement;

import cn.lightfish.sql.ast.SQLTypeMap;
import cn.lightfish.sql.ast.complier.ComplierContext;
import cn.lightfish.sql.ast.complier.ExprComplier;
import cn.lightfish.sql.ast.converter.Converters;
import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.executor.logicExecutor.*;
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

    public StatementDispatcher(MycatConsole console) {
        this.console = console;
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        console.getContext().setQueryExecutor(console.getComplierContext().getRootQueryComplier().complieRootQuery(x));
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
        List<ValuesClause> valuesList = x.getValuesList();
        List<Object[]> rows = new ArrayList<>(valuesList.size());
        for (ValuesClause valuesClause : valuesList) {
            List<SQLExpr> values = valuesClause.getValues();
            Object[] row = new Object[count];
            for (int i = 0; i < count; i++) {
                SQLExpr valueExpr = values.get(i);
                ValueExpr expr = exprComplier.createExpr(valueExpr);
                row[i] = expr.getValue();
            }
            rows.add(row);
        }
        RootSessionContext context = console.getContext();
        context.rootType = ExecutorType.INSERT;
        console.getContext().setInsertExecutor(new InsertExecutor(table, columnNameList, persistentAttribute, rows.iterator()));
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
        console.getContext().rootType = ExecutorType.DELETE;
        ComplierContext complierContext = console.getComplierContext();

        ExprComplier exprComplier = complierContext.getExprComplier();
        SQLExprTableSource tableSource = (SQLExprTableSource) x.getTableSource();

        MycatTable table = console.getCurrentSchema()
                .getTableByName(tableSource.getSchemaObject().getName());
        complierContext.createColumnAllocator(x);
        BooleanExpr where = (BooleanExpr) exprComplier.createExpr(x.getWhere());
        TableColumnDefinition[] columnDefinition = complierContext.getColumnAllocatior()
                .getLeafTableColumnDefinition(tableSource);
        Executor executor = complierContext.getTableSourceComplier().createLeafTableSource(tableSource, 0, -1, ExecutorType.DELETE);
        console.getContext().setDeleteExecutor(new DeleteExecutor(columnDefinition,table,new FilterExecutor(executor, where),Collections.emptyMap()));
        return super.visit(x);
    }

    public Executor getConsoleResult() {
        switch (console.getContext().rootType) {
            case QUERY:
           return console.getContext().queryExecutor;
            case UPDATE:
                return console.getContext().updateExecutor;
            case DELETE:
                return console.getContext().deleteExecutor;
            case INSERT:
                return console.getContext().insertExecutor;
                default:throw new UnsupportedOperationException();
        }
    }
}