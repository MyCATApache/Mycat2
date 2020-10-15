package io.mycat;

import io.mycat.config.ClusterRootConfig;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MetaClusterCurrent {
  public static  final   ConcurrentMap<Class,Object> context = new ConcurrentHashMap<>();
    public static  <T> T wrapper(Class<T> tClass){
        return (T)Objects.requireNonNull(context.get(tClass));
    }

    public static void register(Class interfaceo,Object clusterRootConfig) {
        context.put(interfaceo,clusterRootConfig);
    }
    public static void register(Object clusterRootConfig) {
        context.put(clusterRootConfig.getClass(),clusterRootConfig);
    }

    public static boolean exist(Class interceptorRuntimeClass) {
        return context.containsKey(interceptorRuntimeClass);
    }
}