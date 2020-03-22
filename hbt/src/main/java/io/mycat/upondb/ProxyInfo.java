package io.mycat.upondb;

import lombok.Getter;


@Getter
public class ProxyInfo {
    private final String targetName;
    String sql;
    boolean forUpdate;

    public ProxyInfo(String targetName, String sql,boolean forUpdate) {
        this.targetName = targetName;
        this.sql = sql;
        this.forUpdate = forUpdate;
    }
}