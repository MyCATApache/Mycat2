package cn.lightfish.sql.ast;

import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import io.mycat.schema.MycatTable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RootExecutionContext {

  public Object[] scope = new Object[32];
  public final Map<Integer, Class> scopeType = new HashMap<>();
  Executor rootExecutor;
  BooleanExpr rootFilter;

  public ValueExpr createField(SQLTableSource tableSource, SQLColumnDefinition resolvedColumn) {
    return null;
  }

  public MycatTable getTable(String tableName) {
    return null;
  }

  public boolean hasNext() {
    return rootExecutor.hasNext();
  }

  public void createScopeSize(int size) {
    if (scope.length < size) {
      scope = new Object[size + 1];
    }
    Arrays.fill(scope, null);
  }

  public void createNullTableSource() {
  }

  public boolean hasDatasource() {
    return rootExecutor != null;
  }

  public Object getGlobalVariant(String name) {
    return name;
  }

  public Object getSessionVariant(String name) {
    return name;
  }
}