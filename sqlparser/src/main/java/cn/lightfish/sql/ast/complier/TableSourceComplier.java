package cn.lightfish.sql.ast.complier;

import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.executor.logicExecutor.ContextExecutor;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import cn.lightfish.sql.schema.MycatSchemaManager;
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

  private final ComplierContext complierContext;

  public TableSourceComplier(ComplierContext context) {
    this.complierContext = context;
  }

  public Executor createLeafTableSource(SQLExprTableSource tableSource, SQLExpr where,
      long offset, long rowCount) {
    String schema = tableSource.getSchemaObject().getSchema().getName();
    String tableName = tableSource.getTableName();
    LogicLeafTableExecutor tableExecuter = MycatSchemaManager.INSTANCE
        .getLogicLeafTableSource(schema, tableName,
            complierContext.getColumnAllocatior()
                .getLeafTableColumnDefinition(tableSource),
            offset, rowCount);
    if (tableExecuter != null) {
      complierContext.registerLeafExecutor(tableExecuter);
      return new ContextExecutor(this.complierContext.runtimeContext, tableExecuter,
          this.complierContext.getColumnAllocatior().getTableStartIndex(tableSource));
    } else {
      throw new UnsupportedOperationException();
    }
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

}