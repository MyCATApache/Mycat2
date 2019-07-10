package io.mycat.compute;

public abstract class Expression<T> implements Cloneable {

  protected final OperateType operateType;

  public Expression(OperateType operateType) {
    this.operateType = operateType;
  }

  public abstract T clone();

  public abstract OperateType getOperateType();

}