package cn.lightfish.sql.ast.expr.numberExpr;

import cn.lightfish.sql.ast.expr.ValueExpr;
import java.math.BigDecimal;

public interface BigDecimalExpr extends ValueExpr<BigDecimal> {

  @Override
  default public Class<BigDecimal> getType() {
    return BigDecimal.class;
  }
}