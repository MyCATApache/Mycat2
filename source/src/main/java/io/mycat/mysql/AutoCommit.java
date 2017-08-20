package io.mycat.mysql;

/**
 * Created by ynfeng on 2017/8/19.
 */
public enum AutoCommit {
    ON("SET autocommit = 1;"),
    OFF("SET autocommit = 0;");

    AutoCommit(String cmd) {
        this.cmd = cmd;
    }

    private String cmd;

    public String getCmd() {
        return cmd;
    }
}
