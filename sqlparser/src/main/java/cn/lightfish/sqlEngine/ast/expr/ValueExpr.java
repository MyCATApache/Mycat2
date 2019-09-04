package cn.lightfish.sqlEngine.ast.expr;

public interface ValueExpr<T extends Comparable<T>> {

  Class<T> getType();

  T getValue();
}