package io.mycat.lib;

import cn.lightfish.pattern.InstructionSet;
import io.mycat.lib.impl.ProxyLib;
import io.mycat.lib.impl.Response;

public class ProxyExport implements InstructionSet {
    public static Response useSchemaThenResponseOk(String schema) {
        return ProxyLib.useSchemaThenResponseOk(schema);
    }


    public static Response responseOk() {
        return ProxyLib.responseOk;
    }

    public static Response proxyQueryOnDatasource(String dataSource,String sql) {
        return ProxyLib.proxyQueryOnDatasource(dataSource,sql );
    }

    public static Response setTransactionIsolationThenResponseOk(String text) {
        return ProxyLib.setTransactionIsolationThenResponseOk(text);
    }
}