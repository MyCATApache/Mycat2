package io.mycat;

import lombok.Getter;

@Getter
public enum ExecuteType {
    QUERY(false),
    QUERY_MASTER(true),
    INSERT(true),
    UPDATE(true),
    ;
    public static ExecuteType DEFAULT = ExecuteType.QUERY_MASTER;
    private boolean master;

    ExecuteType(boolean master) {
        this.master = master;
    }

}