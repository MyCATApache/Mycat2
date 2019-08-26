package cn.lightfish.sql.ast.expr;

public interface ValueExpr<T extends Comparable<T>> {

  Class<T> getType();

  T getValue();
}