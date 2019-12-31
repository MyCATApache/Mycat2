package io.mycat.sqlEngine.ast.expr;

public interface ValueExpr<T extends Comparable<T>> {

  Class<T> getType();

  T getValue();
}