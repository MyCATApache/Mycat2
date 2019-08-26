package cn.lightfish.sql.ast.complier;

import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.executor.logicExecutor.ContextExecutor;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.FilterExecutor;
import cn.lightfish.sql.executor.logicExecutor.LogicTableExecutor;
import cn.lightfish.sql.schema.MycatSchemaManager;
import cn.lightfish.sql.schema.SimpleColumnDefinition;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLValuesTableSource;

public class TableSourceComplier {

  private ComplierContext complierContext;

  public TableSourceComplier(ComplierContext context) {
    this.complierContext = context;
  }

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

  public Executor createTableSource(SQLTableSource tableSource, SQLExpr where,
      long offset, long rowCount) {
    if (tableSource == null) {
      return null;
    }
    if (tableSource instanceof SQLExprTableSource) {
      SQLExprTableSource table = (SQLExprTableSource) tableSource;
     return createTableSource(table, where, offset, rowCount);
    } else if (tableSource instanceof SQLSubqueryTableSource) {

    } else if (tableSource instanceof SQLJoinTableSource) {

    } else if (tableSource instanceof SQLUnionQueryTableSource) {

    } else if (tableSource instanceof SQLUnnestTableSource) {

    } else if (tableSource instanceof SQLLateralViewTableSource) {

    } else if (tableSource instanceof SQLValuesTableSource) {

    } else {
      throw new UnsupportedOperationException();
    }
    return null;
  }

  public Executor createTableSource(SQLExprTableSource tableSource, SQLExpr where,
      long offset, long rowCount) {
    String schema = tableSource.getSchemaObject().getSchema().getName();
    String tableName = tableSource.getTableName();
    SimpleColumnDefinition[] mycatColumnDefinitions = complierContext.getColumnAllocatior()
        .getColumnDefinition(tableSource);
    Executor tableExecuter = MycatSchemaManager.INSTANCE
        .getLogicTableSource(complierContext.runtimeContext, schema, tableName, mycatColumnDefinitions,
            offset, rowCount);
    ColumnAllocator columnAllocatior = this.complierContext.getColumnAllocatior();
    tableExecuter =  new ContextExecutor(this.complierContext.runtimeContext, tableExecuter,columnAllocatior.getTableStartIndex(tableSource));
    return tableExecuter;
  }
}