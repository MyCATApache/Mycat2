package io.mycat.sqlparser.util.complie;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.fastsql.sql.ast.expr.SQLInListExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLSomeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelect;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.visitor.SQLASTVisitorAdapter;
import io.mycat.sqlparser.util.MycatSchemaRespository;
import java.util.ArrayList;
import java.util.List;

public class RangeLayoutResolver extends SQLASTVisitorAdapter {

  final List<SQLObject> subQueries = new ArrayList<>();
  final List<SQLObject> opExpr = new ArrayList<>();
  final List<SQLObject> expr = new ArrayList<>();
  final List<SQLObject> orExpr = new ArrayList<>();
  private SchemaObject schemaObject;
  private SQLTableSource tableSource;
  final MycatSchemaRespository schemaRespository;
  final List<RangeVariable> conditions = new ArrayList<>();
  boolean fail = false;

  void failture() {
    this.fail = true;
  }

  public RangeLayoutResolver(SQLTableSource tableSource, MycatSchemaRespository schemaRespository) {
    this.tableSource = tableSource;
    this.schemaRespository = schemaRespository;
  }


  public void endVisit(SQLBinaryOpExpr x) {
    SQLBinaryOperator operator = x.getOperator();
    switch (operator) {
      case BooleanOr:
        orExpr.add(x);
        break;
      case Equality: {
        SQLExpr left = x.getLeft();
        SQLExpr right = x.getRight();
        SQLColumnDefinition column = null;
        Object value = null;
        if (left instanceof SQLName && right instanceof SQLValuableExpr) {
          column = ((SQLName) left).getResolvedColumn();
          value = ((SQLValuableExpr) right).getValue();
        } else if (left instanceof SQLValuableExpr && right instanceof SQLName) {
          value = ((SQLValuableExpr) left).getValue();
          column = ((SQLName) right).getResolvedColumn();
        }
        if (column != null && value != null) {

        }
      }
    }
  }

  @Override
  public void endVisit(SQLInListExpr x) {
    SQLExpr expr = x.getExpr();
    SQLColumnDefinition column = null;
    if (expr!=null&&expr instanceof SQLName){
      column  = ((SQLName) expr).getResolvedColumn();
    }
    List<SQLExpr> targetList = x.getTargetList();
    for (SQLExpr sqlExpr : targetList) {

    }

    super.endVisit(x);
  }

  @Override
  public boolean visit(SQLInSubQueryExpr x) {
    return super.visit(x);
  }

  @Override
  public void endVisit(SQLUnaryExpr x) {
    failture();
    super.endVisit(x);
  }

  @Override
  public boolean visit(SQLBetweenExpr x) {
    SQLExpr testExpr = x.getTestExpr();
    SQLExpr beginExpr = x.getBeginExpr();
    SQLExpr endExpr = x.getEndExpr();
    if (testExpr instanceof SQLName && beginExpr instanceof SQLValuableExpr
        && endExpr instanceof SQLValuableExpr) {
      SQLColumnDefinition column = ((SQLName) testExpr).getResolvedColumn();
    }
    return super.visit(x);
  }

  @Override
  public void endVisit(SQLBetweenExpr x) {
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLSubqueryTableSource x) {
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLInSubQueryExpr x) {
//    subQueries.add(x);
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLSomeExpr x) {
//    subQueries.add(x);
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLSelectQueryBlock x) {
//    subQueries.add(x);
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLSelect select) {
    subQueries.add(select);
    super.endVisit(select);
  }

  @Override
  public void endVisit(SQLSelectStatement selectStatement) {
    super.endVisit(selectStatement);
  }


  @Override
  public boolean visit(SQLSelect x) {
    return super.visit(x);
  }


}