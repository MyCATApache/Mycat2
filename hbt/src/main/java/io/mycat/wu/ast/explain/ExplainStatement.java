package io.mycat.wu.ast.explain;

public class ExplainStatement {
    final ExplainType explainType;
    final ExplainAttributes explainAttributes;
    final String asOut;

    public ExplainStatement(ExplainType explainType, ExplainAttributes explainAttributes, String asOut) {
        this.explainType = explainType;
        this.explainAttributes = explainAttributes;
        this.asOut = asOut;
    }
}