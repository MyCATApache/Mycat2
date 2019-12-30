package cn.lightfish.wu;

public enum Op {
    //SET OPERATORS
    UNION_DISTINCT("unionDistinct"),
    UNION_ALL("unionAll"),
    EXCEPT_DISTINCT("exceptDistinct"),
    EXCEPT_ALL("exceptAll"),
    MINUS_ALL("minusAll"),
    MINUS_DISTINCT("minusDistinct"),
    INTERSECT_DISTINCT("intersectDistinct"),
    INTERSECT_ALL("intersectAll"),

    //relational operators
    FROM("from"),
    MAP("map"),
    FILTER("filter"),
    LIMIT("limit"),
    ORDER("order"),
    GROUP("group"),
    VALUES("values"),
    DISTINCT("distinct"),
    PROJECT("project"),
    INNER_JOIN("innerJoin"),
    LEFT_JOIN("leftJoin"),
    CORRELATE_INNER_JOIN("correlateInnerJoin"),
    CORRELATE_LEFT_JOIN("correlateLeftJoin"),
    RIGHT_JOIN("rightJoin"),
    FULL_JOIN("fillJoin"),
    SEMI_JOIN("semiJoin"),
    ANTI_JOIN("antiJoin"),
    CORRELATE("correlate"),

    // types
    SCHEMA("schema"),
    SCALAR_TYPE("scalarType"),
    FIELD_SCHEMA("fieldSchema"),

    //atoms
    LITERAL("literal"),
    IDENTIFIER("id"),
    PROPERTY("property"),

    //debug
    DESCRIBE("describe"),
    DUMP("dump"),

    // operators
    DOT("dot"),
    EQ("eq"),
    NE("ne"),
    GT("gt"),
    LT("lt"),
    GTE("gte"),
    LTE("lte"),
    PLUS("plus"),
    MINUS("minus"),
    AND("and"),
    OR("or"),
    NOT("not"),
    AS_COLUMNNAME("asColumnName"),
    CAST("cast"),
    FUN("fun"),
    REF("ref"),
    AggregateCall("aggregateCall"),
    REGULAR("regular"),
    ;

    String fun;

    Op(String fun) {
        this.fun = fun;
    }

    public String getFun() {
        return fun;
    }
}