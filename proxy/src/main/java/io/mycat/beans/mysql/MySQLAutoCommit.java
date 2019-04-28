package io.mycat.beans.mysql;

public enum MySQLAutoCommit {
    ON("SET autocommit = 1;"),
    OFF("SET autocommit = 0;");

    MySQLAutoCommit(String cmd) {
        this.cmd = cmd;
    }

    private String cmd;

    public String getCmd() {
        return cmd;
    }
}
