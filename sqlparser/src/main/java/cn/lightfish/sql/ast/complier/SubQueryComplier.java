package cn.lightfish.sql.ast.complier;

import cn.lightfish.sql.executor.logicExecutor.Executor;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import java.util.Collections;
import java.util.List;

public class SubQueryComplier {

  private ComplierContext complierContext;

  public SubQueryComplier(ComplierContext complierContext) {
    this.complierContext = complierContext;
  }
  public Executor createSubQuery(SQLSelectQueryBlock queryBlock,
      SubQueryType type) {
    long row;
    List<SQLSelectItem> selectList = queryBlock.getSelectList();
    SQLExpr where = queryBlock.getWhere();
    SQLTableSource from = queryBlock.getFrom();
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
    Executor tableSource = complierContext.createTableSource(from, where, 0, row);
    return complierContext.getProjectComplier().createProject(selectList, null, tableSource);
  }
}