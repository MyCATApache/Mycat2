package io.mycat.calcite.sqlfunction;

import org.apache.calcite.MycatContext;

public class LastInsertIdFunction {
    public static long eval() {
        return MycatContext.CONTEXT.get().getLastInsertId();
    }
}
