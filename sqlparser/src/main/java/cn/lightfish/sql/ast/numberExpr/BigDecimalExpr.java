package cn.lightfish.sql.ast.numberExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigDecimal;
import java.math.BigInteger;

public interface BigDecimalExpr extends ValueExpr<BigDecimal> {

  @Override
  default public Class<BigDecimal> getType() {
    return BigDecimal.class;
  }
}