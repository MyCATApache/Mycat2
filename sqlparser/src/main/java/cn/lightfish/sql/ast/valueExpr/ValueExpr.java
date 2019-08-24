package cn.lightfish.sql.ast.valueExpr;

public interface ValueExpr<T extends Comparable<T>> {

  Class<T> getType();

   T getValue();
}