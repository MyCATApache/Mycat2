package io.mycat.beans.mysql;

public enum MySQLIsolation {
    READ_UNCOMMITTED("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;"),
    READ_COMMITTED("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;"),
    REPEATED_READ("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;"),
    SERIALIZABLE("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;");

    private String cmd;

    MySQLIsolation(String cmd) {
        this.cmd = cmd;
    }

    public String getCmd() {
        return cmd;
    }
}