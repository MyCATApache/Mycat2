package cn.lightfish.sql.ast.arithmeticExpr.bigIntegerOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.BigIntegerExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigInteger;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigIntegerMultipyExpr implements BigIntegerExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public BigInteger getValue() {
    BigInteger leftValue = (BigInteger) left.getValue();
    BigInteger rightValue = (BigInteger) right.getValue();
    return leftValue .multiply(rightValue);
  }
}