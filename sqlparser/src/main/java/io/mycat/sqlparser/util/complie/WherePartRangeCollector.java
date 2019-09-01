package io.mycat.sqlparser.util.complie;

import static com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator.Equality;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.fastsql.sql.ast.expr.SQLInListExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLListExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelect;
import com.alibaba.fastsql.sql.visitor.SQLASTVisitorAdapter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WherePartRangeCollector extends SQLASTVisitorAdapter {

  final List<SQLObject> subQueries = new ArrayList<>();
  final List<SQLExpr> orExpr = new ArrayList<>();
  final List<SQLExpr> andExpr = new ArrayList<>();
  final Map<SQLColumnDefinition, List<RangeVariable>> conditions = new HashMap<>();
  ;
  final List<String> failMessage = new ArrayList<>();

  void failture(String format, Object... args) {
    failMessage.add(MessageFormat.format(format, args));
  }

  public WherePartRangeCollector() {

  }

  private void collectIndexExpression(SQLExpr x) {
    if (x instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) x;
      switch (binaryOpExpr.getOperator()) {
        case Equality: {
          SQLExpr leftExpr = binaryOpExpr.getLeft();
          SQLExpr rightExpr = binaryOpExpr.getRight();
          if (leftExpr instanceof SQLListExpr && rightExpr instanceof SQLListExpr) {
            List<SQLExpr> leftList = ((SQLListExpr) binaryOpExpr.getLeft()).getItems();
            List<SQLExpr> rightList = ((SQLListExpr) binaryOpExpr.getRight()).getItems();
            for (int i = 0; i < leftList.size(); i++) {
              SQLExpr left = leftList.get(i);
              SQLExpr rifht = rightList.get(i);
              SQLBinaryOpExpr sqlBinaryOpExpr = new SQLBinaryOpExpr(left, Equality, rifht);
              collectIndexExpression(sqlBinaryOpExpr);
            }
          } else {
            SQLColumnDefinition column = null;
            Object value = null;
            if (leftExpr instanceof SQLName && rightExpr instanceof SQLValuableExpr) {
              column = ((SQLName) leftExpr).getResolvedColumn();
              value = ((SQLValuableExpr) rightExpr).getValue();
            } else if (leftExpr instanceof SQLValuableExpr && rightExpr instanceof SQLName) {
              value = ((SQLValuableExpr) leftExpr).getValue();
              column = ((SQLName) rightExpr).getResolvedColumn();
            }
            if (column != null && value != null) {
              collectRangeVariable(new RangeVariable(column, RangeVariableType.EQUAL, value));
            }
          }
          break;
        }
        case BooleanAnd: {

        }
      }
    } else if (x instanceof SQLBetweenExpr) {
      SQLBetweenExpr betweenExpr = (SQLBetweenExpr) x;
      if (betweenExpr.isNot()) {
        return;
      }
      SQLExpr testExpr = betweenExpr.getTestExpr();
      SQLExpr beginExpr = betweenExpr.getBeginExpr();
      SQLExpr endExpr = betweenExpr.getEndExpr();
      if (testExpr instanceof SQLName && beginExpr instanceof SQLValuableExpr
          && endExpr instanceof SQLValuableExpr) {
        SQLColumnDefinition column = ((SQLName) testExpr).getResolvedColumn();
        Object beginValue = ((SQLValuableExpr) beginExpr).getValue();
        Object endValue = ((SQLValuableExpr) endExpr).getValue();
        if (column != null && beginValue != null && endValue != null) {
          collectRangeVariable(
              new RangeVariable(column, RangeVariableType.RANGE, beginValue, endValue));
        }
      }
    } else if (x instanceof SQLInListExpr) {
      SQLInListExpr inListExpr = (SQLInListExpr) x;
      SQLExpr expr = inListExpr.getExpr();
      SQLColumnDefinition column = null;
      if (expr != null && expr instanceof SQLName) {
        column = ((SQLName) expr).getResolvedColumn();
      }
      if (column != null) {
        List<SQLExpr> targetList = inListExpr.getTargetList();
        for (SQLExpr sqlExpr : targetList) {
          if (sqlExpr instanceof SQLValuableExpr) {
            Object value = ((SQLValuableExpr) sqlExpr).getValue();
            collectRangeVariable(
                new RangeVariable(column, RangeVariableType.EQUAL, value));
          } else {
            continue;
          }
        }

      }
    }
  }

  private void collectAndOrExpression(SQLExpr x) {
    if (x instanceof SQLBinaryOpExprGroup) {
      SQLBinaryOpExprGroup sqlBinaryOpExprGroup = (SQLBinaryOpExprGroup) x;
      switch (sqlBinaryOpExprGroup.getOperator()) {
        case BooleanAnd:
          andExpr.add(sqlBinaryOpExprGroup);
          break;
        case BooleanOr:
          andExpr.add(sqlBinaryOpExprGroup);
          break;
      }
    }
    if (x instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) x;
      switch (sqlBinaryOpExpr.getOperator()) {
        case BooleanOr:
          orExpr.add(sqlBinaryOpExpr);
          break;
        case BooleanAnd:
          andExpr.add(sqlBinaryOpExpr);
          break;
      }
    }
  }

  void collectRangeVariable(RangeVariable rangeVariable) {
    List<RangeVariable> list = conditions.get(rangeVariable.getColumn());
    if (list == null) {
      conditions.put(rangeVariable.getColumn(),list= new ArrayList<>());
    }
    list.add(rangeVariable);
  }


  public boolean visit(SQLBinaryOpExprGroup x) {
    collectAndOrExpression(x);
    collectIndexExpression(x);
    return super.visit(x);
  }

  public boolean visit(SQLBinaryOpExpr x) {
    collectAndOrExpression(x);
    collectIndexExpression(x);
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLInListExpr x) {
    collectIndexExpression(x);
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLInSubQueryExpr x) {
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLUnaryExpr x) {
    SQLUnaryOperator operator = x.getOperator();
    if (operator == SQLUnaryOperator.NOT || operator == SQLUnaryOperator.Not) {
      failture("not operator in {0}", x);
      return false;
    }
    return super.visit(x);
  }

  @Override
  public boolean visit(SQLBetweenExpr x) {
    collectIndexExpression(x);
    return super.visit(x);
  }

  @Override
  public void endVisit(SQLSelect select) {
    subQueries.add(select);
    super.endVisit(select);
  }

  ///////////////////////////////////////////////////////////////////////////////////
  public List<SQLObject> getSubQueries() {
    return subQueries;
  }

  public List<SQLExpr> getOrExpr() {
    return orExpr;
  }

  public List<SQLExpr> getAndExpr() {
    return andExpr;
  }

  public Map<SQLColumnDefinition, List<RangeVariable>> getConditions() {
    return conditions;
  }

  public List<String> getFailMessage() {
    return failMessage;
  }
}