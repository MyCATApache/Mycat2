package io.mycat;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MetaCluster extends Snapshot {
    final Map<Class, Object> context = new HashMap<>();

    public MetaCluster(Long timestamp) {
        super(timestamp);
    }

    private static final ThreadLocal CURRENT = ThreadLocal.withInitial(() -> null);

    public static MetaCluster getCurrent() {
        return (MetaCluster) CURRENT.get();
    }

    public static void ensure() {

    }

    public void add(Object o) {
        Objects.requireNonNull(o);
        Class<?> aClass = o.getClass();
        context.put(aClass,o);
    }

    public <T> T get(Class<T> tClass){
        return (T)context.get(tClass);
    }
}