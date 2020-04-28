package io.mycat;

import lombok.Getter;

@Getter
public enum ExecuteType {
    QUERY(false),
    QUERY_MASTER(true),
    INSERT(true),
    UPDATE(true),
//    AUTO(true),
//        RANDOM_QUERY(false),
//        BROADCAST_UPDATE(true),
    ;
    private boolean master;

    public static ExecuteType DEFAULT = ExecuteType.QUERY_MASTER;

    ExecuteType(boolean master) {
        this.master = master;
    }

}