package cn.lightfish.sql.ast.expr.booleanExpr;

public class BooleanValueExpr implements BooleanExpr {

  private final Boolean booleanValue;

  public BooleanValueExpr(boolean booleanValue) {
    this.booleanValue = booleanValue;
  }

 public final static BooleanValueExpr TRUE = new BooleanValueExpr(true);
  public final static BooleanValueExpr FALSE = new BooleanValueExpr(false);


  @Override
  public Boolean test() {
    return booleanValue;
  }
}