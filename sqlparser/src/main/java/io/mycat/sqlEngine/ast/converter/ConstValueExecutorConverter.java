/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.sqlEngine.ast.converter;

import com.alibaba.fastsql.sql.ast.expr.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanValueExpr;
import io.mycat.sqlEngine.ast.expr.dateExpr.DateConstExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.BigDecimalConstExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.DoubleConstExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.LongConstExpr;
import io.mycat.sqlEngine.ast.expr.stringExpr.StringConstExpr;
import io.mycat.sqlEngine.ast.expr.valueExpr.NullConstExpr;

import java.math.BigDecimal;
/**
 * @author Junwen Chen
 **/
class ConstValueExecutorConverter extends MySqlASTVisitorAdapter {

  public ValueExpr value;

  public ValueExpr getValue() {
    return value == null ? NullConstExpr.INSTANCE : value;
  }

  @Override
  public boolean visit(SQLBigIntExpr x) {
    value = new LongConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLBinaryExpr x) {
    value = new StringConstExpr(x.getText());
    return false;
  }

  @Override
  public boolean visit(SQLBooleanExpr x) {
    value = new BooleanValueExpr(x.getBooleanValue());
    return false;
  }

  @Override
  public boolean visit(SQLCharExpr x) {
    value = new StringConstExpr(x.getText());
    return false;
  }

  @Override
  public boolean visit(SQLDateExpr x) {
    value = new DateConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLDateTimeExpr x) {
    value = new DateConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLDecimalExpr x) {
    value = new BigDecimalConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLDoubleExpr x) {
    value = new DoubleConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLFloatExpr x) {
    value = new DoubleConstExpr(x.getValue().doubleValue());
    return false;
  }

  @Override
  public boolean visit(SQLHexExpr x) {
    value = new StringConstExpr(x.getHex());
    return false;
  }

  @Override
  public boolean visit(SQLIntegerExpr x) {
    value = new LongConstExpr(x.getNumber().longValue());
    return false;
  }

  @Override
  public boolean visit(SQLJSONExpr x) {
    value = new StringConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLNullExpr x) {
    value = NullConstExpr.INSTANCE;
    return false;
  }

  @Override
  public boolean visit(SQLNumberExpr x) {
    BigDecimal number = (BigDecimal) x.getNumber();
    value = new BigDecimalConstExpr(number);
    return false;
  }

  @Override
  public boolean visit(SQLRealExpr x) {
    value = new DoubleConstExpr(x.getNumber().doubleValue());
    return false;
  }

  @Override
  public boolean visit(SQLSmallIntExpr x) {
    value = new LongConstExpr(x.getNumber().longValue());
    return false;
  }

  @Override
  public boolean visit(SQLTimeExpr x) {
    value = new DateConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLTimestampExpr x) {
    value = new DateConstExpr(x.getValue());
    return false;
  }

  @Override
  public boolean visit(SQLTinyIntExpr x) {
    value = new LongConstExpr(x.getValue().longValue());
    return false;
  }

  @Override
  public boolean visit(MySqlCharExpr x) {
    value = new StringConstExpr(x.getText());
    return false;
  }
}