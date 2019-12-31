package io.mycat.wu.ast.explain;

import io.mycat.wu.ast.base.Identifier;

public class ExplainOp {
    public static void main(String[] args) {
        explan(ExplainType.WITH_IMPLEMENTATION, ExplainAttributes.EXCLUDING_ATTRIBUTES, "JSON");
    }

    public static ExplainStatement explan(ExplainType explainType, ExplainAttributes excludingAttributes, String outType) {
        return new ExplainStatement(explainType, excludingAttributes, outType);
    }

    public static ExplainStatement explan(String explainType, String excludingAttributes, String outType) {
        return explan(ExplainType.valueOf(explainType), ExplainAttributes.valueOf(excludingAttributes), outType);
    }

    public static ExplainStatement explan(Identifier explainType, Identifier excludingAttributes, Identifier outType) {
        return explan(explainType.getValue(), excludingAttributes.getValue(), outType.getValue());
    }
}