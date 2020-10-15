package io.mycat.hbt4;

import io.mycat.MycatDataContext;

public class MycatContext {
    public Object[] values;
    public static final ThreadLocal<MycatDataContext> CONTEXT = ThreadLocal.withInitial(() -> null);
}