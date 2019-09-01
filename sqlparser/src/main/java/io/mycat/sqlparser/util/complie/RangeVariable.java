package io.mycat.sqlparser.util.complie;

import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;

public class RangeVariable {
  private SQLColumnDefinition column;
  private RangeVariableType operator;
  private Object value;
  private Object optionValue = null;

  public RangeVariable(SQLColumnDefinition column,
      RangeVariableType operator,Object value) {
    assert operator == RangeVariableType.EQUAL;
    this.column = column;
    this.operator = operator;
    this.value = value;
  }
  public RangeVariable(SQLColumnDefinition column,
      RangeVariableType operator,Object value,Object optionValue) {
    assert operator == RangeVariableType.RANGE;
    this.column = column;
    this.operator = operator;
    this.value = value;
    this.optionValue = optionValue;
  }
  public SQLColumnDefinition getColumn() {
    return column;
  }

  public RangeVariableType getOperator() {
    return operator;
  }

  public Object getBegin() {
    return value;
  }

  public Object getEnd() {
    return optionValue;
  }
}