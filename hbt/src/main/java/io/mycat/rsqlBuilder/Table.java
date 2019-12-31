package io.mycat.rsqlBuilder;

public interface Table {
    Table filter(RowExpr expr);
}