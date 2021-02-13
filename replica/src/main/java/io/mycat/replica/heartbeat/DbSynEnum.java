package io.mycat.replica.heartbeat;

public enum DbSynEnum {
    DB_SYN_ERROR(-1),
    DB_SYN_NORMAL(1);
    final int value;

    DbSynEnum(int value) {
        this.value = value;
    }
}
