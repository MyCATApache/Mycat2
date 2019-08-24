package cn.lightfish.sql.ast.arithmeticExpr.bigIntegerOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.BigIntegerExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigInteger;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigIntegerSignExpr implements BigIntegerExpr {

  private final RootExecutionContext context;
  private final ValueExpr value;

  @Override
  public BigInteger getValue() {
    BigInteger value = (BigInteger) this.value.getValue();
    return value.negate();
  }
}