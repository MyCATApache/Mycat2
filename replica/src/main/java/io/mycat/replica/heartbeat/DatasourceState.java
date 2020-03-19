package io.mycat.replica.heartbeat;

public enum DatasourceState {
    DB_SYN_ERROR(-1),
    DB_SYN_NORMAL(1),
    OK_STATUS(1),
    ERROR_STATUS(-1),
    TIMEOUT_STATUS(-2),
    INIT_STATUS(0);
    final int value;

    DatasourceState(int value) {
        this.value = value;
    }
}