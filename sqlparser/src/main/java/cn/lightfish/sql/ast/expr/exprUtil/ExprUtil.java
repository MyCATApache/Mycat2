package cn.lightfish.sql.ast.expr.exprUtil;

import cn.lightfish.sql.ast.expr.ValueExpr;
import cn.lightfish.sql.ast.expr.dateExpr.DateConstExpr;
import cn.lightfish.sql.ast.expr.numberExpr.BigDecimalConstExpr;
import cn.lightfish.sql.ast.expr.numberExpr.DoubleConstExpr;
import cn.lightfish.sql.ast.expr.numberExpr.LongConstExpr;
import cn.lightfish.sql.ast.expr.stringExpr.StringConstExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import java.math.BigDecimal;
import java.sql.Date;

public class ExprUtil {

  public static ValueExpr transfor(Object value) {
    if (value instanceof String) {
      return new StringConstExpr(value.toString());
    } else if (value instanceof Long) {
      return new LongConstExpr((Long) value);
    } else if (value instanceof BigDecimal) {
      return new BigDecimalConstExpr((BigDecimal) value);
    } else if (value instanceof Double) {
      return new DoubleConstExpr((Double) value);
    }
    Class<?> type = value.getClass();
    if (type == java.sql.Date.class) {
      return new DateConstExpr((Date) value);
    } else if (type == java.util.Date.class) {
      return new DateConstExpr((Date) value);
    }else if (java.util.Date.class.isAssignableFrom(type)){
      return new DateConstExpr((Date) value);
    }
    throw new UnsupportedOperationException();
  }
  public static ValueExpr transfor(SQLValuableExpr valuableExpr) {
    ConstValueExecutorTransfor transfor = new ConstValueExecutorTransfor();
    valuableExpr.accept(transfor);
    return transfor.getValue();
  }

}