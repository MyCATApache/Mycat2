package cn.lightfish.sql.ast;

import cn.lightfish.sql.ast.booleanExpr.BooleanValueExpr;
import cn.lightfish.sql.ast.dateExpr.DateValueExpr;
import cn.lightfish.sql.ast.numberExpr.BigDecimalConstExpr;
import cn.lightfish.sql.ast.numberExpr.DoubleConstExpr;
import cn.lightfish.sql.ast.numberExpr.LongConstExpr;
import cn.lightfish.sql.ast.stringExpr.StringConstExpr;
import cn.lightfish.sql.ast.valueExpr.NullConstExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
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
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

public class ExecutorUtil {

  public static ValueExpr transfor(SQLValuableExpr valuableExpr) {
    ValueExecutorTransfor transfor = new ValueExecutorTransfor();
    valuableExpr.accept(transfor);
    return transfor.getValue();
  }

  static class ValueExecutorTransfor extends MySqlASTVisitorAdapter {

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
      value = new DateValueExpr(x.getValue());
      return false;
    }

    @Override
    public boolean visit(SQLDateTimeExpr x) {
      value = new DateValueExpr(x.getValue());
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
      value = new LongConstExpr(x.getNumber().longValue());
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
      value = new DateValueExpr(x.getValue());
      return false;
    }

    @Override
    public boolean visit(SQLTimestampExpr x) {
      value = new DateValueExpr(x.getValue());
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
}