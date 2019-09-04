package cn.lightfish.sqlEngine.ast.expr.numberExpr;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import java.math.BigDecimal;

public interface BigDecimalExpr extends ValueExpr<BigDecimal> {

  @Override
  default public Class<BigDecimal> getType() {
    return BigDecimal.class;
  }
}