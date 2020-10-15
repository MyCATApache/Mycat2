package io.mycat.calcite.sqlfunction;

import io.mycat.hbt4.MycatContext;

public class LastInsertIdFunction {
    public static long eval() {
        return MycatContext.CONTEXT.get().getLastInsertId();
    }
}
