package cn.lightfish.rsqlBuilder;

public interface Table {
    Table filter(RowExpr expr);
}