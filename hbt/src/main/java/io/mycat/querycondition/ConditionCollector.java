/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.querycondition;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class ConditionCollector extends MySqlASTVisitorAdapter {
    private boolean failureIndeterminacy = false;
    private final LinkedList<QueryDataRange> stack = new LinkedList<>();
    private QueryDataRange root;
    private boolean isJoin;


    public ConditionCollector() {

    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        isJoin = true;
        return super.visit(x);
    }

    @Override
    public void endVisit(SQLJoinTableSource x) {
        isJoin = false;
        super.endVisit(x);
    }

    private void addEqualValue(ColumnValue columnValue) {
        QueryDataRange peek = stack.peek();
        QueryDataRange queryDataRange = peek;
        if (isJoin) {
            queryDataRange.joinEqualValues.add(columnValue);
        } else {
            queryDataRange.equalValues.add(columnValue);
        }
    }

    private void addRangeValues(ColumnRangeValue columnRangeValue) {
        QueryDataRange queryDataRange = Objects.requireNonNull(stack.peek());
        if (isJoin) {
            queryDataRange.joinEangeValues.add(columnRangeValue);
        } else {
            queryDataRange.rangeValues.add(columnRangeValue);
        }
    }

    public void failureBecauseIndeterminacy(SQLExpr sqlExpr) {
        failureIndeterminacy = true;
        Objects.requireNonNull(stack.peek()).messageList.add(sqlExpr.toString());
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        QueryDataRange queryDataRange = new QueryDataRange(x);
        if (root == null) {
            root = queryDataRange;
        }
        stack.push(queryDataRange);
        return super.visit(x);
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {
        QueryDataRange pop = stack.pop();
        if (stack.isEmpty()) {
            root = pop;
        } else {
            stack.peek().children.add(pop);
        }
        super.endVisit(x);
    }
    @Override
    public boolean visit(MySqlUpdateStatement x) {
        SQLExprTableSource tableSource = (SQLExprTableSource)x.getTableSource();
        QueryDataRange queryDataRange = new QueryDataRange(tableSource);
        if (root == null) {
            root = queryDataRange;
        }
        stack.push(queryDataRange);
        return super.visit(x);
    }

    @Override
    public void endVisit(MySqlUpdateStatement x) {
        QueryDataRange pop = stack.pop();
        if (stack.isEmpty()) {
            root = pop;
        } else {
            stack.peek().children.add(pop);
        }
        super.endVisit(x);
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        SQLExprTableSource tableSource = (SQLExprTableSource)x.getTableSource();
        QueryDataRange queryDataRange = new QueryDataRange(tableSource);
        if (root == null) {
            root = queryDataRange;
        }
        stack.push(queryDataRange);
        return super.visit(x);
    }

    @Override
    public void endVisit(MySqlDeleteStatement x) {
        QueryDataRange pop = stack.pop();
        if (stack.isEmpty()) {
            root = pop;
        } else {
            stack.peek().children.add(pop);
        }
        super.endVisit(x);
    }


    public boolean visit(SQLAggregateExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLAllExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLAnyExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLArrayExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLBigIntExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLBinaryOpExprGroup x) {
        SQLBinaryOperator operator = x.getOperator();
        switch (operator) {
            default:
            case BooleanOr:
                failureBecauseIndeterminacy(x);
                break;
            case BooleanAnd:
                List<SQLExpr> items = x.getItems();
                int size = items.size();
                for (int i = 0; i < size; i++) {
                    SQLExpr first = items.get(i);
                    if (first instanceof SQLBinaryOpExpr) {
                        SQLBinaryOpExpr leftBinaryOpExpr = (SQLBinaryOpExpr) first;
                        ColumnValue firstValue = getWhereRangeLeftValue(leftBinaryOpExpr.getLeft(),
                                leftBinaryOpExpr.getOperator(),
                                leftBinaryOpExpr.getRight());
                        if (firstValue != null) {
                            for (int j = i + 1; j < size; j++) {
                                SQLExpr second = items.get(j);
                                addWhereRange(firstValue, second);
                            }
                        }
                    }
                }
        }
        return super.visit(x);
    }

    private void addWhereRange(ColumnValue firstValue, SQLExpr second) {
        if (second instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr rightBinaryOpExpr = (SQLBinaryOpExpr) second;
            ColumnValue secondValue = getWhereRangeRightValue(rightBinaryOpExpr.getLeft(),
                    rightBinaryOpExpr.getOperator(),
                    rightBinaryOpExpr.getRight());
            if (secondValue != null) {
                if (firstValue.tableSource.equals(secondValue.tableSource) &&
                        firstValue.column.equals(secondValue.column) && firstValue.operator.equals(secondValue.operator)) {
                    addRangeValues(new ColumnRangeValue(firstValue.column, firstValue.value, secondValue.value, firstValue.tableSource));
                }
            }
        }
    }

    //id <=1
    private ColumnValue getWhereRangeRightValue(SQLExpr leftExpr, SQLBinaryOperator operator, SQLExpr rightExpr) {
        if (operator != SQLBinaryOperator.LessThanOrEqual) {
            return null;
        }
        SQLColumnDefinition column = null;
        Object value = null;
        if (leftExpr instanceof SQLName && rightExpr instanceof SQLValuableExpr) {
            column = ((SQLName) leftExpr).getResolvedColumn();
            value = ((SQLValuableExpr) rightExpr).getValue();
        }
        SQLTableSource tableSource = null;
        if (column != null && value != null) {
            tableSource = Converters.getColumnTableSource(leftExpr);
        }
        return new ColumnValue(column, operator, value, tableSource);
    }

    //1<= id
    private ColumnValue getWhereRangeLeftValue(SQLExpr leftExpr, SQLBinaryOperator operator, SQLExpr rightExpr) {
        boolean left = true;
        SQLColumnDefinition column = null;
        Object value = null;
        if (leftExpr instanceof SQLValuableExpr && rightExpr instanceof SQLName) {
            column = ((SQLName) rightExpr).getResolvedColumn();
            value = ((SQLValuableExpr) leftExpr).getValue();
            left = false;
        }
        SQLTableSource tableSource = null;
        if (column != null && value != null) {
            tableSource = Converters.getColumnTableSource(rightExpr);
        }
        //1<= id
        if (column != null && operator != null && value != null && tableSource != null) {
            return new ColumnValue(column, operator, value, tableSource);
        } else {
            return null;
        }
    }


    public boolean visit(SQLCurrentOfCursorExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLCharExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLContainsExpr x) {
        boolean not = x.isNot();
        return super.visit(x);
    }

    public boolean visit(SQLDateExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLDateTimeExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLDbLinkExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLDecimalExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLDefaultExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLDoubleExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLExistsExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLExtractExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLFlashbackExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLFloatExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLGroupingSetExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLIntervalExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLJSONExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLListExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLMatchAgainstExpr x) {
        failureBecauseIndeterminacy(x);
        return super.visit(x);
    }

    public boolean visit(SQLNotExpr x) {
        failureBecauseIndeterminacy(x);
        return super.visit(x);
    }

    public boolean visit(SQLNCharExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLPropertyExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLRealExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLSequenceExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLSizeExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLSmallIntExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLSomeExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLTimeExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLTimestampExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLTinyIntExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLValuesExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLVariantRefExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLInSubQueryExpr x) {
        boolean not = x.isNot();
        if (not) {
            failureBecauseIndeterminacy(x);
        }
        return super.visit(x);
    }


    public boolean visit(SQLBinaryOpExpr x) {
        SQLBinaryOperator operator = x.getOperator();
        SQLExpr leftExpr = x.getLeft();
        SQLExpr rightExpr = x.getRight();
        if (operator == SQLBinaryOperator.BooleanAnd) {
            if (leftExpr instanceof SQLBinaryOpExpr && rightExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr first = (SQLBinaryOpExpr) leftExpr;
                ColumnValue leftValue = getWhereRangeLeftValue(first.getLeft(), first.getOperator(), first.getRight());
                if (leftValue != null) {
                    addWhereRange(leftValue, rightExpr);
                }
            }
        } else if (operator == SQLBinaryOperator.Equality) {
            if ((leftExpr instanceof SQLName) && (rightExpr instanceof SQLValuableExpr)) {
                SQLColumnDefinition columnDef = Converters.getColumnDef(leftExpr);
                SQLTableSource columnTableSource = Converters.getColumnTableSource(leftExpr);
                Object value = ((SQLValuableExpr) rightExpr).getValue();
                addEqualValue(new ColumnValue(columnDef, SQLBinaryOperator.Equality, value, columnTableSource));
            } else if ((leftExpr instanceof SQLValuableExpr) && (rightExpr instanceof SQLName)) {
                SQLColumnDefinition columnDef = Converters.getColumnDef(rightExpr);
                SQLTableSource columnTableSource = Converters.getColumnTableSource(rightExpr);
                Object value = ((SQLValuableExpr) leftExpr).getValue();
                addEqualValue(new ColumnValue(columnDef, SQLBinaryOperator.Equality, value, columnTableSource));
            }
        }
        return super.visit(x);
    }


    public boolean visit(SQLUnaryExpr x) {
        SQLUnaryOperator operator = x.getOperator();
        switch (operator) {
            case Plus:
                break;
            case Negative:
                break;
            case Not:
                failureBecauseIndeterminacy(x);
                break;
            case Compl:
                break;
            case Prior:
                break;
            case ConnectByRoot:
                break;
            case BINARY:
                break;
            case RAW:
                break;
            case NOT:
                failureBecauseIndeterminacy(x);
                break;
            case Pound:
                break;
        }
        return super.visit(x);
    }

    public boolean visit(SQLIntegerExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLNumberExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLHexExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLBinaryExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLCaseExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        boolean not = x.isNot();
        if (not) {
            failureBecauseIndeterminacy(x);
        } else {
            SQLExpr testExpr = x.getTestExpr();
            SQLExpr beginExpr = x.getBeginExpr();
            SQLExpr endExpr = x.getEndExpr();
            if (testExpr instanceof SQLName && beginExpr instanceof SQLValuableExpr
                    && endExpr instanceof SQLValuableExpr) {
                SQLColumnDefinition column = ((SQLName) testExpr).getResolvedColumn();
                SQLTableSource source = Converters.getColumnTableSource(testExpr);
                if (source != null) {
                    Object beginValue = ((SQLValuableExpr) beginExpr).getValue();
                    Object endValue = ((SQLValuableExpr) endExpr).getValue();
                    if (column != null && beginValue != null && endValue != null) {
                        addRangeValues(new ColumnRangeValue(column, beginValue, endValue, source));
                    }
                }
            }
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        boolean not = x.isNot();
        if (not) {
            failureBecauseIndeterminacy(x);
        } else {
            SQLExpr expr = x.getExpr();
            SQLColumnDefinition column = null;
            SQLTableSource columnTableSource = null;
            if (expr instanceof SQLName) {
                column = Converters.getColumnDef(expr);
                columnTableSource = Converters.getColumnTableSource(expr);
            }
            if (column != null && columnTableSource != null) {
                List<SQLExpr> targetList = x.getTargetList();
                for (SQLExpr sqlExpr : targetList) {
                    if (sqlExpr instanceof SQLValuableExpr) {
                        Object value = ((SQLValuableExpr) sqlExpr).getValue();
                        addEqualValue(new ColumnValue(column, SQLBinaryOperator.Equality, value, columnTableSource));
                    } else {
                        continue;
                    }
                }

            }
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLNullExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLQueryExpr x) {
        return super.visit(x);
    }

    public boolean visit(SQLIdentifierExpr x) {
        return super.visit(x);
    }

    @Override
    public boolean visit(SQLBooleanExpr x) {
        return super.visit(x);
    }

    public boolean isFailureIndeterminacy() {
        return failureIndeterminacy;
    }

    public void setFailureIndeterminacy(boolean failureIndeterminacy) {
        this.failureIndeterminacy = failureIndeterminacy;
    }


    public static void main(String[] args) {

    }

    public QueryDataRange getRootQueryDataRange() {
        return root == null ? stack.peek() : root;
    }

    public LinkedList<QueryDataRange> getStack() {
        return stack;
    }
}
