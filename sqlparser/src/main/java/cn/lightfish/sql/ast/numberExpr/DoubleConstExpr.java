package cn.lightfish.sql.ast.numberExpr;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleConstExpr implements DoubleExpr {

  final Double value;

  @Override
  public Double getValue() {
    return value;
  }
}