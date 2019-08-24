package io.mycat.sqlparser;

import cn.lightfish.sql.ast.Complier;
import cn.lightfish.sql.ast.Executor;
import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.SQLTypeMap;
import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
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
import com.alibaba.fastsql.util.JdbcUtils;
import io.mycat.schema.MycatColumnDefinition;
import io.mycat.schema.MycatPartition;
import io.mycat.schema.MycatTable;
import io.mycat.schema.MycatConsole;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.jdbc.JdbcConvention;

public class StatementDispatcher extends MySqlASTVisitorAdapter {

  final MycatConsole console;
  private final Complier complier;
  MycatConsoleResult consoleResult;
  final RootExecutionContext context = new RootExecutionContext();
  public StatementDispatcher(MycatConsole console) {
    this.console = console;
    this.complier = new Complier();
  }

  @Override
  public boolean visit(SQLSelectStatement x) {
    Executor projectExecutor = complier.complieRootQuery(x);
    MycatColumnDefinition[]  columnNames = projectExecutor.columnDefList();
    System.out.println("column:"+Arrays.toString(columnNames));
    while (projectExecutor.hasNext()){
      Object[] next = projectExecutor.next();
      System.out.println(Arrays.toString(next));
    }
    return super.visit(x);
  }


  @Override
  public boolean visit(MySqlInsertStatement x) {
    MycatTable table = console.getCurrnetSchema().getTableByName(x.getTableName().getSimpleName());
    List<SQLExpr> columns = x.getColumns();
    int count = columns.size();
    MycatPartition partition = table.getPartition();
    Map<Integer, List<ValuesClause>> route = new HashMap<>();
    if (partition == null) {

    } else {
      String partitionCoulumn = partition.getDbPartitionCoulumn();
      String partitionCoulumn2 = partition.getTablePartitionCoulumn();
      int partitionCoulumnIndex = -1;
      int partitionCoulumnIndex2 = -1;
      for (int i = 0; i < count; i++) {
        SQLExpr column = columns.get(i);
        if (column instanceof SQLName) {
          SQLColumnDefinition columnDefinition = ((SQLName) column).getResolvedColumn();
          String columnName = columnDefinition.getColumnName();
          if (columnName.equalsIgnoreCase(partitionCoulumn)) {
            partitionCoulumnIndex = i;
          } else if (columnName.equalsIgnoreCase(partitionCoulumn2)) {
            partitionCoulumnIndex2 = i;
          }
        }
      }
      List<ValuesClause> valuesList = x.getValuesList();
      for (ValuesClause valuesClause : valuesList) {
        List<SQLExpr> values = valuesClause.getValues();
        SQLValuableExpr valuableExpr = (SQLValuableExpr) values.get(partitionCoulumnIndex);
        Object value = valuableExpr.getValue();
        partition.assignment(value);
        if (partitionCoulumn2 != null) {
          if (!partitionCoulumn.equalsIgnoreCase(partitionCoulumn2)) {
            SQLValuableExpr valuableExpr2 = (SQLValuableExpr) values.get(partitionCoulumnIndex2);
            Object value2 = valuableExpr2.getValue();
            partition.assignment(value2);
          }
        }
        int dataNodeIndex = partition.getReturnValue();

        List<ValuesClause> group = route.computeIfAbsent(dataNodeIndex, k -> new ArrayList<>());
        group.add(valuesClause);
      }
    }
    console.insert(table, route, x);
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLCreateDatabaseStatement x) {
    this.consoleResult = console.createSchema(x.getDatabaseName());
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLShowDatabasesStatement x) {
    this.consoleResult = console.showDatabase();
    return super.visit(x);
  }

  @Override
  public boolean visit(MySqlCreateTableStatement x) {
    String tableName = x.getTableSource().getSchemaObject().getName();
    List<SQLTableElement> tableElementList =
        x.getTableElementList() == null ? Collections.emptyList() : x.getTableElementList();
    List<MycatColumnDefinition> columnDefinitions = new ArrayList<>(tableElementList.size());
    String primaryKey = null;
    for (SQLTableElement sqlTableElement : tableElementList) {
      if (sqlTableElement instanceof SQLColumnDefinition) {
        SQLColumnDefinition columnDefinition = (SQLColumnDefinition) sqlTableElement;
        String columnName = columnDefinition.getColumnName();
        SQLDataType dataType = columnDefinition.getDataType();
        MycatColumnDefinition mycatColumnDefinition = new MycatColumnDefinition(columnName,  SQLTypeMap.toClass(dataType.jdbcType()));
        columnDefinitions.add(mycatColumnDefinition);
      } else if (sqlTableElement instanceof SQLColumnPrimaryKey) {
        SQLColumnPrimaryKey columnPrimaryKey = (SQLColumnPrimaryKey) sqlTableElement;
        primaryKey = columnPrimaryKey.getName().getSimpleName();
      }
    }
    SQLMethodInvokeExpr dbPartitionBy = (SQLMethodInvokeExpr) x
        .getDbPartitionBy();//指定分库键和分库算法，不支持按照时间分库；
    if (dbPartitionBy != null) {
      this.consoleResult = console.createTable(new MycatTable(tableName, columnDefinitions,
          getMycatPartition(x, primaryKey, dbPartitionBy)));
    } else {
      this.consoleResult = console
          .createTable(new MycatTable(tableName, columnDefinitions, x.isBroadCast()));
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
    this.consoleResult = console.dropDatabase(x.getDatabaseName());
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLDropTableGroupStatement x) {
    this.consoleResult = console.dropTable(x.getTableGroupName());
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLDropTableStatement x) {
    List<SQLExprTableSource> tableSources = x.getTableSources();
    List<String> nameList = new ArrayList<>();
    for (SQLExprTableSource tableSource : tableSources) {
      nameList.add(tableSource.getTableName());
    }
    this.consoleResult = console.dropTable(nameList);

    return super.visit(x);
  }

  @Override
  public boolean visit(MySqlDeleteStatement x) {
    MycatTable table = console.getCurrnetSchema().getTableByName(x.getTableName().toString());
    SQLExpr where = x.getWhere();
    return super.visit(x);
  }

  public MycatConsoleResult getConsoleResult() {
    return consoleResult;
  }
}