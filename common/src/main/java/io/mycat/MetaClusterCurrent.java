package io.mycat;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class MetaClusterCurrent {
    public static final AtomicReference<Map<Class, Object>> context = new AtomicReference<>(new HashMap<>());

    public static <T> T wrapper(Class<T> tClass) {
        Map<Class, Object> classObjectMap = context.get();
        Object o = classObjectMap.get(tClass);
        return (T) Objects.requireNonNull(o);
    }

    public static void register(Map<Class, Object> newContext) {
        context.set(newContext);
    }

    public static boolean exist(Class interceptorRuntimeClass) {
        return context.get().get(interceptorRuntimeClass) != null;
    }
    public static IdentityHashMap<Class, Object>copyContext(){
        return new IdentityHashMap<>(context.get());
    }
    public static IdentityHashMap<Class, Object>copyContext(Class c, Object o){
        IdentityHashMap<Class, Object> map = new IdentityHashMap<>(context.get());
        map.put(c,o);
        return map;
    }
    public static void register(Class c, Object o) {
       register(copyContext(c,o));
    }
}
