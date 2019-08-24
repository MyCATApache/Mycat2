package cn.lightfish.sql.ast.numberExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigInteger;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigIntegerConstExpr implements ValueExpr<BigInteger> {

  final BigInteger value;

  @Override
  public BigInteger getValue() {
    return value;
  }

  @Override
  public Class<BigInteger> getType() {
    return BigInteger.class;
  }
}