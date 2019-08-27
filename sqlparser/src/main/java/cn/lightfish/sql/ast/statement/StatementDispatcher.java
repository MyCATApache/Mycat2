package cn.lightfish.sql.ast.statement;

import cn.lightfish.sql.ast.complier.ComplierContext;
import cn.lightfish.sql.ast.SQLTypeMap;
import cn.lightfish.sql.ast.converter.Converters;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.persistent.InsertPersistent;
import cn.lightfish.sql.persistent.PersistentManager;
import cn.lightfish.sql.schema.MycatSchema;
import cn.lightfish.sql.schema.SimpleColumnDefinition;
import cn.lightfish.sql.schema.MycatConsole;
import cn.lightfish.sql.schema.MycatPartition;
import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.TableColumnDefinition;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropTableGroupStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLTableElement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StatementDispatcher extends MySqlASTVisitorAdapter {

  final MycatConsole console;
  private final ComplierContext complierContext;
  Executor consoleResult;

  public StatementDispatcher(MycatConsole console) {
    this.console = console;
    this.complierContext = new ComplierContext(console.getContext());
  }

  @Override
  public boolean visit(SQLSelectStatement x) {
    Executor projectExecutor = complierContext.getRootQueryComplier().complieRootQuery(x);
    SimpleColumnDefinition[] columnNames = projectExecutor.columnDefList();
    System.out.println("column:" + Arrays.toString(columnNames));
    while (projectExecutor.hasNext()) {
      Object[] next = projectExecutor.next();
      System.out.println(Arrays.toString(next));
    }
    return super.visit(x);
  }


  @Override
  public boolean visit(MySqlInsertStatement x) {
    MycatTable table = console.getCurrentSchema().getTableByName(x.getTableName().getSimpleName());
    List<SQLExpr> columns = x.getColumns();
    int count = columns.size();
//    String[] columnNameList = new String[count];
//    for (int i = 0; i < count; i++) {
//      columnNameList[i] = Converters.getColumnName(columns.get(i));
//    }
    Map<String,Object> persistentAttribute = new HashMap<>();
    InsertPersistent insertPersistent = PersistentManager.INSTANCE.getInsertPersistent(console,table,null, persistentAttribute);
    List<ValuesClause> valuesList = x.getValuesList();
    for (ValuesClause valuesClause : valuesList) {
      List<SQLExpr> values = valuesClause.getValues();
      Object[] row = new Object[count];
      for (int i = 0; i < count; i++) {
        SQLExpr valueExpr = values.get(i);
        SQLValuableExpr valuableExpr = (SQLValuableExpr) valueExpr;
        row[i] = valuableExpr.getValue();
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
      console.createTable(new MycatTable(currnetSchema,tableName, columnDefinitions,
          getMycatPartition(x, primaryKey, dbPartitionBy)));
    } else {
      console.createTable(new MycatTable(currnetSchema,tableName, columnDefinitions, x.isBroadCast()));
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
    return new MycatPartition(dbMethodName, dbPartitionCoulumn,
        tableMethodName,
        tablePartitionCoulumn,
        tablePartitions);
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
    MycatTable table = console.getCurrentSchema().getTableByName(x.getTableName().toString());
    SQLExpr where = x.getWhere();
    return super.visit(x);
  }

  public Executor getConsoleResult() {
    return consoleResult;
  }
}