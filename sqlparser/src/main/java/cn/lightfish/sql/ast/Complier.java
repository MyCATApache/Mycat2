package cn.lightfish.sql.ast;

import static cn.lightfish.sql.ast.expr.MycatParser.CACHE_REPOSITORY;

import cn.lightfish.sql.executor.Executor;
import cn.lightfish.sql.executor.ProjectExecutor;
import cn.lightfish.sql.ast.expr.exprUtil.ExprUtil;
import cn.lightfish.sql.ast.expr.arithmeticExpr.bigDecimalOperator.BigDecimalAddExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.bigDecimalOperator.BigDecimalDivisionExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.bigDecimalOperator.BigDecimalMultipyExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.bigDecimalOperator.BigDecimalSubtractExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.doubleOperator.DoubleAddExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.doubleOperator.DoubleDivisionExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.doubleOperator.DoubleMultipyExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.doubleOperator.DoubleSubtractExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.longOperator.LongAddExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.longOperator.LongDivisionExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.longOperator.LongMultipyExpr;
import cn.lightfish.sql.ast.expr.arithmeticExpr.longOperator.LongSubtractExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExistsExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.compareExpr.BooleanEqualityExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.compareExpr.BooleanLessThanExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.compareExpr.BooleanNotEqualityExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.logicalExpr.BooleanAndExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.logicalExpr.BooleanOrExpr;
import cn.lightfish.sql.ast.optimizer.ColumnOptimizer;
import cn.lightfish.sql.ast.optimizer.ColumnOptimizer.SelectColumn;
import cn.lightfish.sql.ast.optimizer.SubqueryOptimizer;
import cn.lightfish.sql.ast.optimizer.SubqueryOptimizer.CorrelatedQuery;
import cn.lightfish.sql.ast.expr.dateExpr.DateExpr;
import cn.lightfish.sql.executor.ContextExecutor;
import cn.lightfish.sql.executor.FilterExecutor;
import cn.lightfish.sql.executor.OnlyProjectExecutor;
import cn.lightfish.sql.ast.expr.functionExpr.FunctionManager;
import cn.lightfish.sql.ast.expr.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.expr.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.expr.numberExpr.LongExpr;
import cn.lightfish.sql.ast.expr.stringExpr.StringConstExpr;
import cn.lightfish.sql.ast.expr.stringExpr.StringExpr;
import cn.lightfish.sql.ast.expr.valueExpr.NullConstExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLCastExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLExistsExpr;
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
import com.alibaba.fastsql.sql.optimizer.Optimizers;
import cn.lightfish.sql.schema.MycatColumnDefinition;
import cn.lightfish.sql.schema.MycatSchemaManager;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class Complier {

  RootExecutionContext context = new RootExecutionContext();
  private final HashMap<Object, Integer> columnIndexMap = new HashMap<>();
  private final Map<Object, List<SQLColumnDefinition>> tableSourceColumnMap = new HashMap<>();
  private final HashMap<Object, Integer> tableSourceColumnStartIndexMap = new HashMap<>();
  private List<CorrelatedQuery> correlateQueries;
  private List<MySqlSelectQueryBlock> normalQueries;

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

  public void createTableSource(SQLTableSource tableSource, SQLExpr where,
      List<SQLColumnDefinition> columnDefinitions, long offset, long rowCount) {
    if (tableSource == null) {
      return;
    }
    if (tableSource instanceof SQLExprTableSource) {
      SQLExprTableSource table = (SQLExprTableSource) tableSource;
      createTableSource(table, where, columnDefinitions, offset, rowCount);
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

  public void createTableSource(SQLExprTableSource tableSource, SQLExpr where,
      List<SQLColumnDefinition> columnDefinitions, long offset, long rowCount) {
    String schema = tableSource.getSchemaObject().getSchema().getName();
    String tableName = tableSource.getTableName();

    MycatColumnDefinition[] mycatColumnDefinitions = new MycatColumnDefinition[columnDefinitions
        .size()];
    for (int i = 0; i < mycatColumnDefinitions.length; i++) {
      SQLColumnDefinition columnDefinition = columnDefinitions.get(i);
      mycatColumnDefinitions[i] = new MycatColumnDefinition(columnDefinition.getColumnName(),
          SQLTypeMap.toClass(columnDefinition.jdbcType()));
    }
    Executor tableExecuter = MycatSchemaManager.INSTANCE
        .getTableSource(schema, tableName, mycatColumnDefinitions, offset, rowCount);
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

      Class leftExprType = leftExpr.getType();
      Class rightExprType = rightExpr.getType();
      if (leftExprType != rightExprType) {
        if ((Number.class.isAssignableFrom(leftExprType) && (Number.class
            .isAssignableFrom(rightExprType)))) {
          leftExpr = createCast(leftExpr, Double.class);
          rightExpr = createCast(rightExpr, Double.class);
        }
      }
      checkReturnType(leftExpr, rightExpr, leftExpr.getType(), sqlExpr);
      switch (binaryOpExpr.getOperator()) {
        case BooleanOr:
          checkReturnType(leftExpr, rightExpr, Boolean.class, sqlExpr);
          return new BooleanOrExpr(context, (BooleanExpr) leftExpr, (BooleanExpr) rightExpr);
        case BooleanAnd:
          checkReturnType(leftExpr, rightExpr, Boolean.class, sqlExpr);
          return new BooleanAndExpr(context, (BooleanExpr) leftExpr, (BooleanExpr) rightExpr);
        case Equality:
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case LessThanOrGreater:
          return new BooleanNotEqualityExpr(context, leftExpr, rightExpr);
        case LessThan:
          return new BooleanLessThanExpr(context, leftExpr, rightExpr);
        case LessThanOrEqual:
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case GreaterThan:
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case GreaterThanOrEqual:
          checkReturnType(leftExpr, rightExpr, leftExpr.getType(), sqlExpr);
          return new BooleanEqualityExpr(context, leftExpr, rightExpr);
        case Add:
          if (leftExpr.getType() == Long.class) {
            return new LongAddExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == Double.class) {
            return new DoubleAddExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == BigDecimal.class) {
            return new BigDecimalAddExpr(context, leftExpr, rightExpr);
          }
          throw new UnsupportedOperationException();
        case Subtract:
          if (leftExpr.getType() == Long.class) {
            return new LongSubtractExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == Double.class) {
            return new DoubleSubtractExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == BigDecimal.class) {
            return new BigDecimalSubtractExpr(context, leftExpr, rightExpr);
          }
          throw new UnsupportedOperationException();
        case Multiply:
          if (leftExpr.getType() == Long.class) {
            return new LongMultipyExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == Double.class) {
            return new DoubleMultipyExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == BigDecimal.class) {
            return new BigDecimalMultipyExpr(context, leftExpr, rightExpr);
          }
          throw new UnsupportedOperationException();
        case Divide:
          if (leftExpr.getType() == Long.class) {
            return new LongDivisionExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == Double.class) {
            return new DoubleDivisionExpr(context, leftExpr, rightExpr);
          }
          if (leftExpr.getType() == BigDecimal.class) {
            return new BigDecimalDivisionExpr(context, leftExpr, rightExpr);
          }
          throw new UnsupportedOperationException();
        default:
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
      return ExprUtil.transfor((SQLValuableExpr) sqlExpr);
    } else if (sqlExpr instanceof SQLVariantRefExpr) {
      SQLVariantRefExpr variantRefExpr = (SQLVariantRefExpr) sqlExpr;
      return createVariantRef(variantRefExpr);
    } else if (sqlExpr instanceof SQLMethodInvokeExpr) {
      return createMethod((SQLMethodInvokeExpr) sqlExpr);
    } else if (sqlExpr instanceof SQLCastExpr) {
      SQLCastExpr expr = (SQLCastExpr) sqlExpr;
      ValueExpr value = createExpr(expr.getExpr());
      Class<?> targetType = SQLTypeMap.toClass(expr.getDataType().jdbcType());
      return createCast(value, targetType);
    } else if (sqlExpr instanceof SQLExistsExpr) {
      SQLExistsExpr existsExpr = (SQLExistsExpr) sqlExpr;
      return createSQLExistsExpr(existsExpr);
    }
    throw new UnsupportedOperationException();
  }

  public BooleanExpr createSQLExistsExpr(SQLExistsExpr existsExpr) {
    return new BooleanExistsExpr(
        createSubQuery(existsExpr.getSubQuery().getQueryBlock(), SubQueryType.EXISTS),
        existsExpr.isNot());
  }

  public Executor createSubQuery(SQLSelectQueryBlock queryBlock,
      SubQueryType type) {
    long row;
    List<SQLSelectItem> selectList = queryBlock.getSelectList();
    SQLExpr where = queryBlock.getWhere();
    SQLTableSource from = queryBlock.getFrom();
    List<SQLColumnDefinition> sqlColumnDefinitions = this.tableSourceColumnMap.get(from);
    switch (type) {
      case TABLE:
        row = -1;
        break;
      case SCALAR:
        if (selectList.size() != 1) {
          throw new UnsupportedOperationException();
        }
        row = 2;
        break;
      case ROW:
        row = 2;
        break;
      case EXISTS:
        row = -1;
        selectList = Collections.emptyList();
        sqlColumnDefinitions = Collections.emptyList();
        break;
      case COLUMN:
        if (selectList.size() != 1) {
          throw new UnsupportedOperationException();
        }
        row = -1;
        break;
      default:
        throw new UnsupportedOperationException();
    }
    createTableSource(from, where, sqlColumnDefinitions, 0, row);
    return createProject(selectList, null);
  }

  public ValueExpr createCast(ValueExpr value, Class<?> targetType) {
    Objects.requireNonNull(targetType);
    Class orginClass = value.getType();
    if (orginClass == targetType) {
      return value;
    } else if (targetType.equals(String.class)) {
      return (StringExpr) () -> {
        Comparable v = value.getValue();
        return v == null ? null : v.toString();
      };
    } else if (targetType.equals(Long.class)) {
      if (Number.class.isAssignableFrom(orginClass)) {
        return (LongExpr) () -> {
          Number v = (Number) value.getValue();
          return v == null ? null : v.longValue();
        };
      } else if (String.class == orginClass) {
        return (LongExpr) () -> {
          String v = (String) value.getValue();
          return v == null ? null : Long.parseLong(v);
        };
      }
    } else if (targetType.equals(Double.class)) {
      if (Number.class.isAssignableFrom(orginClass)) {
        return (DoubleExpr) () -> {
          Number v = (Number) value.getValue();
          return v == null ? null : v.doubleValue();
        };
      } else if (String.class == orginClass) {
        return (DoubleExpr) () -> {
          String v = (String) value.getValue();
          return v == null ? null : Double.parseDouble(v);
        };
      }
    } else if (targetType.equals(BigDecimal.class)) {
      if (orginClass == Long.class) {
        return (BigDecimalExpr) () -> {
          Long v = (Long) value.getValue();
          return v == null ? null : new BigDecimal(v);
        };
      } else if (orginClass == Double.class) {
        return (BigDecimalExpr) () -> {
          Double v = (Double) value.getValue();
          return v == null ? null : new BigDecimal(v);
        };
      } else if (orginClass == String.class) {
        return (BigDecimalExpr) () -> {
          String v = (String) value.getValue();
          return v == null ? null : new BigDecimal(v);
        };
      }
    } else if (targetType.equals(Date.class) || targetType == java.util.Date.class) {
      if (orginClass == String.class) {
        return (DateExpr) () -> {
          String v = (String) value.getValue();
          return v == null ? null : Date.valueOf(v);
        };
      }
    } else if (targetType.equals(Timestamp.class)) {
      if (orginClass == String.class) {
        return (DateExpr) () -> {
          String v = (String) value.getValue();
          return v == null ? null : Timestamp.valueOf(v);
        };
      }
    } else if (targetType.equals(Time.class)) {
      if (orginClass == String.class) {
        return (DateExpr) () -> {
          String v = (String) value.getValue();
          return v == null ? null : Time.valueOf(v);
        };
      }
    }
    return NullConstExpr.INSTANCE;
  }

  public ValueExpr createVariantRef(SQLVariantRefExpr variantRefExpr) {
    if (variantRefExpr.isGlobal()) {
      return new StringConstExpr((String) context.getGlobalVariant(variantRefExpr.getName()));
    } else if (variantRefExpr.isSession()) {
      return new StringConstExpr((String) context.getSessionVariant(variantRefExpr.getName()));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  public ValueExpr createMethod(SQLMethodInvokeExpr methodInvokeExpr) {
    String methodName = methodInvokeExpr.getMethodName();
    List<SQLExpr> arguments = methodInvokeExpr.getArguments();
    if (arguments == null || arguments.isEmpty()) {
      Object value = FunctionManager.INSTANCE.getFunctionByName(methodName).apply(null);
      return ExprUtil.transfor(value);
    } else {
      Object[] args = new Object[arguments.size()];
      for (int i = 0; i < args.length; i++) {
        args[i] = createExpr(arguments.get(i)).getValue();
      }
      Object value = FunctionManager.INSTANCE.getFunctionByName(methodName).apply(args);
      return ExprUtil.transfor(value);
    }
  }

  public void checkReturnType(ValueExpr leftExpr, ValueExpr rightExpr,
      Class clazz, SQLExpr sqlExpr) {
    if (leftExpr.getType().equals(clazz) && clazz.equals(rightExpr.getType())) {

    } else {
      throw new ClassCastException(MessageFormat
          .format("{0} left is {1} right is {2}", sqlExpr, leftExpr.getType(),
              rightExpr.getType()));
    }
  }

  public <T extends Comparable<T>> ValueExpr<T> getFieldExecutor(
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

  public Executor createProject(List<SQLSelectItem> selectItems,
      List<String> aliasList) {
    int size = aliasList != null ? aliasList.size() : selectItems.size();
    MycatColumnDefinition[] columnDefinitions = new MycatColumnDefinition[size];
    ValueExpr[] exprs = new ValueExpr[size];
    for (int i = 0; i < size; i++) {
      SQLSelectItem item = selectItems.get(i);
      exprs[i] = createExpr(item.getExpr());
      columnDefinitions[i] = new MycatColumnDefinition(
          aliasList != null ? aliasList.get(i) : item.computeAlias(), exprs[i].getType());
    }
    return createProjectExecutor(columnDefinitions, exprs);
  }

  public Executor createProjectExecutor(MycatColumnDefinition[] columnDefinitions,
      ValueExpr[] exprs) {
    if (context.hasDatasource()) {
      return new ProjectExecutor(columnDefinitions, exprs, context.rootExecutor);
    } else {
      Object[] res = new Object[exprs.length];
      for (int i = 0; i < exprs.length; i++) {
        res[i] = exprs[i].getValue();
      }
      return new OnlyProjectExecutor(columnDefinitions, res);
    }
  }

  public void initExecuteScope(Map<SQLTableSource, SelectColumn> datasourceMap) {
    this.columnIndexMap.clear();
    this.context.scopeType.clear();
    this.tableSourceColumnStartIndexMap.clear();
    for (Entry<SQLTableSource, SelectColumn> entry : datasourceMap.entrySet()) {
      SQLTableSource key1 = entry.getKey();
      SelectColumn value = entry.getValue();
      tableSourceColumnStartIndexMap.put(key1, columnIndexMap.size());
      List<SQLColumnDefinition> columnList = null;
      tableSourceColumnMap.put(key1, columnList = new ArrayList<SQLColumnDefinition>());
      for (Entry<SQLExpr, SQLColumnDefinition> definitionEntry : value
          .getColumnMap().entrySet()) {
        SQLColumnDefinition columnDefinition = definitionEntry.getValue();
        if (!columnIndexMap.containsKey(columnDefinition)) {
          int index = columnIndexMap.size();
          columnIndexMap.put(columnDefinition, index);
          columnList.add(columnDefinition);
          context.scopeType.put(index, SQLTypeMap.toClass(columnDefinition.jdbcType()));
        }
      }
    }
    context.createScopeSize(columnIndexMap.size());
  }

  public Executor complieRootQuery(SQLSelectStatement x) {
    SQLSelectQueryBlock rootQuery = x.getSelect().getQueryBlock();
    List<String> aliasList = new ArrayList<>();
    for (SQLSelectItem sqlSelectItem : rootQuery.getSelectList()) {
      aliasList.add(sqlSelectItem.toString());
    }
    Optimizers.optimize(x, DbType.mysql, CACHE_REPOSITORY);
    ColumnOptimizer columnCollector = new ColumnOptimizer();
    x.accept(columnCollector);
    this.initExecuteScope(columnCollector.getDatasourceMap());
    SubqueryOptimizer subqueryCollector = new SubqueryOptimizer();
    x.accept(subqueryCollector);
    this.correlateQueries = subqueryCollector.getCorrelateQueries();
    this.normalQueries = subqueryCollector.getNormalQueries();
    SQLTableSource from = rootQuery.getFrom();
    createTableSource(from, rootQuery.getWhere(), tableSourceColumnMap.get(from), 0, -1);
    return createProject(rootQuery.getSelectList(), aliasList);
  }
}