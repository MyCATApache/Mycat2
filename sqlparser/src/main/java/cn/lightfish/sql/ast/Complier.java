package cn.lightfish.sql.ast;

import cn.lightfish.sql.ast.arithmeticExpr.longOperator.LongAddExpr;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.booleanExpr.compareExpr.BooleanEqualityExpr;
import cn.lightfish.sql.ast.booleanExpr.compareExpr.BooleanLessThanExpr;
import cn.lightfish.sql.ast.booleanExpr.compareExpr.BooleanNotEqualityExpr;
import cn.lightfish.sql.ast.booleanExpr.logicalExpr.BooleanAndExpr;
import cn.lightfish.sql.ast.booleanExpr.logicalExpr.BooleanOrExpr;
import cn.lightfish.sql.ast.collector.ColumnCollector;
import cn.lightfish.sql.ast.collector.ColumnCollector.SelectColumn;
import cn.lightfish.sql.ast.collector.SubqueryCollector;
import cn.lightfish.sql.ast.collector.SubqueryCollector.CorrelatedQuery;
import cn.lightfish.sql.ast.function.FunctionManager;
import cn.lightfish.sql.ast.stringExpr.StringConstExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import io.mycat.schema.MycatColumnDefinition;
import io.mycat.schema.MycatSchemaManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Complier {

  RootExecutionContext context = new RootExecutionContext();
  private Map<SQLTableSource, SelectColumn> datasourceMap;
  private final HashMap<Object, Integer> columnIndexMap = new HashMap<>();
  private final HashMap<Object, Integer> tableSourceColumnStartIndexMap = new HashMap<>();

  public void createTableSource(SQLTableSource tableSource, ValueExpr where) {

  }

  public void createTableSource(SQLSubqueryTableSource tableSource) {

  }

  public void createTableSource(SQLJoinTableSource tableSource) {

  }

  public void createTableSource(SQLUnionQueryTableSource tableSource) {

  }

  public void createTableSource(SQLUnnestTableSource tableSource) {

  }

  public void createTableSource(SQLLateralViewTableSource tableSource) {

  }

  public void createTableSource(SQLValuesTableSource tableSource) {

  }

  public void createTableSource(SQLTableSource tableSource, SQLExpr where) {
    if (tableSource == null) {
      context.createNullTableSource();
      return;
    }
    if (tableSource instanceof SQLExprTableSource) {
      SQLExprTableSource table = (SQLExprTableSource) tableSource;
      createTableSource(table, where);
    } else if (tableSource instanceof SQLSubqueryTableSource) {

    } else if (tableSource instanceof SQLJoinTableSource) {

    } else if (tableSource instanceof SQLUnionQueryTableSource) {

    } else if (tableSource instanceof SQLUnnestTableSource) {

    } else if (tableSource instanceof SQLLateralViewTableSource) {

    } else if (tableSource instanceof SQLValuesTableSource) {

    } else {
      throw new UnsupportedOperationException();
    }
  }

  private void createTableSource(SQLExprTableSource tableSource, SQLExpr where) {
    String schema = tableSource.getSchemaObject().getSchema().getName();
    String tableName = tableSource.getTableName();
    Executor tableExecuter = MycatSchemaManager.INSTANCE
        .getTableSource(schema, tableName);
    context.rootExecutor = new ContextExecutor(this.context, tableExecuter,
        tableSourceColumnStartIndexMap.get(tableSource));
    if (where != null) {
      context.rootFilter = (BooleanExpr) createExpr(where);
      context.rootExecutor = new FilterExecutor(context.rootExecutor, context.rootFilter);
    }


  }

  public ValueExpr createExpr(SQLExpr sqlExpr) {
    if (sqlExpr instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
      ValueExpr leftExpr = createExpr(binaryOpExpr.getLeft());
      ValueExpr rightExpr = createExpr(binaryOpExpr.getRight());
      switch (binaryOpExpr.getOperator()) {
        case BooleanOr:
          checkReturnType(leftExpr, rightExpr, Boolean.class);
          return new BooleanOrExpr(context, (BooleanExpr) leftExpr, (BooleanExpr) rightExpr);
        case BooleanAnd:
          checkReturnType(leftExpr, rightExpr, Boolean.class);
          return new BooleanAndExpr(context, (BooleanExpr) leftExpr, (BooleanExpr) rightExpr);
        case Equality:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case LessThanOrGreater:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new BooleanNotEqualityExpr(context, leftExpr, rightExpr);
        case LessThan:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new BooleanLessThanExpr(context, leftExpr, rightExpr);
        case LessThanOrEqual:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case GreaterThan:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case GreaterThanOrEqual:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case Add:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType());
          return new LongAddExpr(context, leftExpr, rightExpr);
        default:
          throw new UnsupportedOperationException();
      }
    } else if (sqlExpr instanceof SQLIdentifierExpr) {
      SQLIdentifierExpr expr = (SQLIdentifierExpr) sqlExpr;
      return getFieldExecutor(expr.getResolvedColumn());
    } else if (sqlExpr instanceof SQLPropertyExpr) {
      SQLPropertyExpr expr = (SQLPropertyExpr) sqlExpr;
      return getFieldExecutor(expr.getResolvedColumn());
    } else if (sqlExpr instanceof SQLName) {
      throw new UnsupportedOperationException();
    } else if (sqlExpr instanceof SQLValuableExpr) {
      return ExecutorUtil.transfor((SQLValuableExpr) sqlExpr);
    } else if (sqlExpr instanceof SQLVariantRefExpr) {
      SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) sqlExpr;
      return createVariantRef(variantRefExpr);
    } else if (sqlExpr instanceof SQLMethodInvokeExpr) {
      return createMethod((SQLMethodInvokeExpr) sqlExpr);
    }
    throw new UnsupportedOperationException();
  }

  private ValueExpr createVariantRef(SQLVariantRefExpr variantRefExpr) {
    if (variantRefExpr.isGlobal()) {
      return new StringConstExpr((String) context.getGlobalVariant(variantRefExpr.getName()));
    } else if (variantRefExpr.isSession()) {
      return new StringConstExpr((String) context.getSessionVariant(variantRefExpr.getName()));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private ValueExpr createMethod(SQLMethodInvokeExpr methodInvokeExpr) {
    String methodName = methodInvokeExpr.getMethodName();
    List<SQLExpr> arguments = methodInvokeExpr.getArguments();
    if (arguments == null || arguments.isEmpty()) {
      Object value = FunctionManager.INSTANCE.getFunctionByName(methodName).apply(null);
      return ExecutorUtil.transfor(value);
    } else {
      Object[] args = new Object[arguments.size()];
      for (int i = 0; i < args.length; i++) {
        args[i] = createExpr(arguments.get(i)).getValue();
      }
      Object value = FunctionManager.INSTANCE.getFunctionByName(methodName).apply(args);
      return ExecutorUtil.transfor(value);
    }
  }

  private void checkReturnType(ValueExpr leftExpr, ValueExpr rightExpr,
      Class clazz) {
    if (leftExpr.getType().equals(clazz) && clazz.equals(rightExpr.getType())) {

    } else {
      throw new ClassCastException();
    }
  }

  private <T extends Comparable<T>> ValueExpr<T> getFieldExecutor(
      SQLColumnDefinition resolvedColumn) {
    int index = columnIndexMap.getOrDefault(resolvedColumn, -1);
    Class type = context.scopeType.get(index);
    return new ValueExpr<T>() {
      @Override
      public Class<T> getType() {
        return type;
      }

      @Override
      public T getValue() {
        return (T) context.scope[index];
      }
    };
  }


  public Executor complieNormalQuery(MySqlSelectQueryBlock normalQuery) {
    createTableSource(normalQuery.getFrom(), normalQuery.getWhere());
    return createProject(normalQuery.getSelectList());
  }

  private Executor createProject(List<SQLSelectItem> selectItems) {
    MycatColumnDefinition[] columnDefinitions = new MycatColumnDefinition[selectItems.size()];
    ValueExpr[] exprs = new ValueExpr[selectItems.size()];
    for (int i = 0; i < exprs.length; i++) {
      SQLSelectItem item = selectItems.get(i);
      String name = item.getAlias();
      if (name == null) {
        name = item.toString();
      }
      exprs[i] = createExpr(item.getExpr());
      columnDefinitions[i] = new MycatColumnDefinition(name, exprs[i].getType());
    }
    if (context.hasDatasource()) {
      return new ProjectExecutor(columnDefinitions, exprs, context.rootExecutor);
    } else {
      Object[] res = new Object[exprs.length];
      for (int i = 0; i < exprs.length; i++) {
        res[i] = exprs[i].getValue();
      }
      return new ScalarProjectExecutor(columnDefinitions, res);
    }

  }

  public void initExecuteScope(Map<SQLTableSource, SelectColumn> datasourceMap) {
    this.datasourceMap = new HashMap<>(datasourceMap);
    this.columnIndexMap.clear();
    this.context.scopeType.clear();
    this.tableSourceColumnStartIndexMap.clear();
    for (Entry<SQLTableSource, SelectColumn> entry : datasourceMap.entrySet()) {
      SQLTableSource key1 = entry.getKey();
      SelectColumn value = entry.getValue();
      tableSourceColumnStartIndexMap.put(key1, columnIndexMap.size());
      for (Entry<SQLExpr, SQLColumnDefinition> definitionEntry : value
          .getColumnMap().entrySet()) {
        SQLColumnDefinition key = definitionEntry.getValue();
        if (!columnIndexMap.containsKey(key)) {
          int index = columnIndexMap.size();
          columnIndexMap.put(key, index);
          context.scopeType.put(index, SQLTypeMap.toClass(key.jdbcType()));
        }
      }
    }
    context.createScopeSize(columnIndexMap.size());
  }

  public Executor complieRootQuery(SQLSelectStatement x) {
    SQLSelectQueryBlock rootQuery = x.getSelect().getQueryBlock();
    List<SQLSelectItem> selectList = new ArrayList<>(rootQuery.getSelectList());
    ColumnCollector columnCollector = new ColumnCollector();
    x.accept(columnCollector);
    this.initExecuteScope(columnCollector.getDatasourceMap());
    SubqueryCollector subqueryCollector = new SubqueryCollector();
    x.accept(subqueryCollector);
    List<CorrelatedQuery> correlateQueries = subqueryCollector.getCorrelateQueries();
    List<MySqlSelectQueryBlock> normalQueries = subqueryCollector.getNormalQueries();
    for (MySqlSelectQueryBlock normalQuery : normalQueries) {
      Executor executor = this.complieNormalQuery(normalQuery);
    }
    createTableSource(rootQuery.getFrom(), rootQuery.getWhere());
    return createProject(selectList);
  }
}