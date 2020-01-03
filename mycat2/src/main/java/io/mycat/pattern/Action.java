package io.mycat.pattern;

import lombok.Getter;

@Getter
public enum  Action {
    SELECT("select",Type.GLOBAL),
    UPDATE("update",Type.GLOBAL),
    DELETE("delete",Type.GLOBAL),
    INSERT("insert",Type.GLOBAL),
    HBT("hbt",Type.GLOBAL),
    USE_STATEMENT("useStatement",Type.GLOBAL),
    COMMIT("commit",Type.GLOBAL),
    BEGIN("begin",Type.GLOBAL),
    SET_TRANSACTION_ISOLATION("setTransactionIsolation",Type.GLOBAL),
    ROLLBACK("rollback",Type.GLOBAL),

    PROXY_ONLY("proxy",Type.GLOBAL),
    JDBC_QUERY_ONLY("jdbcQuery",Type.GLOBAL),
    EXECUTE("execute",Type.GLOBAL),

    SET_TRANSACTION_TYPE("setTransactionType",Type.GLOBAL),
    SET_AUTOCOMMIT_OFF("setAutoCommitOff",Type.GLOBAL),
    SET_AUTOCOMMIT_ON("setAutoCommitOn",Type.GLOBAL),
    UNKNOWN("unknown",Type.GLOBAL),
    ;
    Type type;
    public String name;
    public static enum Type {
        GLOBAL,QUERY,UPDATE
    }

    Action(String name,Type type) {
        this.type = type;
        this.name = name;
    }
}
