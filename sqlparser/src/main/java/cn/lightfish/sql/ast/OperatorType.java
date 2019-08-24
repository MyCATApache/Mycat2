package cn.lightfish.sql.ast;

public enum OperatorType {
    LESS("<", true),
    GREATER(">", true),
    EQUAL("=", true),
    NOT_EQUAL("<>", true),
    LESS_EQUAL("<=", true),
    GREATER_EQUAL(">=", true),
    AND("and", true),
    OR("or", true),
    ADD("+", false),
    MINUS("-", false),
    MULTIPLY("*", false),
    DIV("/", false),
    NOT("not", true),
    CONCAT("||", false);
    String text;
    boolean compare;

    OperatorType(String text, boolean isBool) {
        this.text = text;
        this.compare = isBool;
    }

    public boolean isCompare() {
        return compare;
    }
}
