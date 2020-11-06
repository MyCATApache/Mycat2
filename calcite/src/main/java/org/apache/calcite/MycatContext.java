package org.apache.calcite;

import io.mycat.MycatDataContext;

public class MycatContext {
    public Object[] values;
    public static final ThreadLocal<MycatDataContext> CONTEXT = ThreadLocal.withInitial(() -> null);
    public static Object getVariable(String name){
        return CONTEXT.get().getVariable(name);
    }
}