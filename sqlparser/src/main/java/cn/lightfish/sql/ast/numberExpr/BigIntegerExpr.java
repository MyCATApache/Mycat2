package cn.lightfish.sql.ast.numberExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigInteger;

public interface BigIntegerExpr extends ValueExpr<BigInteger> {

  @Override
  default public Class<BigInteger> getType() {
    return BigInteger.class;
  }
}