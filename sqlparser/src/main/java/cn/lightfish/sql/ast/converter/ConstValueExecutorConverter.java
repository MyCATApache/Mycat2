package cn.lightfish.sql.ast.converter;

import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanValueExpr;
import cn.lightfish.sql.ast.expr.dateExpr.DateConstExpr;
import cn.lightfish.sql.ast.expr.numberExpr.BigDecimalConstExpr;
import cn.lightfish.sql.ast.expr.numberExpr.DoubleConstExpr;
import cn.lightfish.sql.ast.expr.numberExpr.LongConstExpr;
import cn.lightfish.sql.ast.expr.stringExpr.StringConstExpr;
import cn.lightfish.sql.ast.expr.valueExpr.NullConstExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBigIntExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLCharExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLDateExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLDateTimeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLDecimalExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLDoubleExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLFloatExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLHexExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLJSONExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLNullExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLNumberExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLRealExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLSmallIntExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLTimeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLTinyIntExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.math.BigDecimal;

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