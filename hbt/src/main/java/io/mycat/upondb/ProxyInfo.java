package io.mycat.upondb;

import lombok.Getter;


@Getter
public class ProxyInfo {
    private final String targetName;
    String sql;
    boolean updateOpt;

    public ProxyInfo(String targetName, String sql,boolean updateOpt) {
        this.targetName = targetName;
        this.sql = sql;
        this.updateOpt = updateOpt;
    }
}