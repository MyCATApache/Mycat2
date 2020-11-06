package io.mycat.calcite.prepare;

import io.mycat.upondb.ProxyInfo;

public interface Proxyable {
    public ProxyInfo tryGetProxyInfo();
}