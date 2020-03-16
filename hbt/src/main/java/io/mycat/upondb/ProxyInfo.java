package io.mycat.upondb;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class ProxyInfo {
    String target;
    String sql;
    boolean update;
}