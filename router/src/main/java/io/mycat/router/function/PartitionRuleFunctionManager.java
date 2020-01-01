package io.mycat.router.function;

import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.router.RuleFunction;

import java.util.Collections;
import java.util.Map;

public enum PartitionRuleFunctionManager {
    INSTANCE;
//    private final ConcurrentHashMap<String, Factory> functions = new ConcurrentHashMap<>();
//
//    public void putFunction(SharingFuntionRootConfig.ShardingFuntion funtion) {
//        ////////////////////////////////////check/////////////////////////////////////////////////
//        Objects.requireNonNull(funtion.getName(), "name of function can not be empty");
//        Objects.requireNonNull(funtion.getClazz(), "poolName of function can not be empty");
//        ////////////////////////////////////check/////////////////////////////////////////////////
//        putRuleAlgorithm(funtion);
//    }

//    public void removeFunction(String name) {
//        functions.remove(name);
//    }

    public static RuleFunction createFunction(String name, String clazz)
            throws Exception {
        Class<?> clz = Class.forName(clazz);
        //判断是否继承AbstractPartitionAlgorithm
        if (!RuleFunction.class.isAssignableFrom(clz)) {
            throw new IllegalArgumentException("rule function must implements "
                    + RuleFunction.class.getName() + ", name=" + name);
        }
        return (RuleFunction) clz.getDeclaredConstructor().newInstance();
    }

    public static RuleFunction getRuleAlgorithm(SharingFuntionRootConfig.ShardingFuntion funtion)
            throws Exception {
        Map<String, String> properties = funtion.getProperties();
        properties = (properties == null) ? Collections.emptyMap() : properties;
        funtion.setProperties(properties);
        RuleFunction rootFunction = createFunction(funtion.getName(), funtion.getClazz());
        rootFunction.callInit(funtion.getProperties(), funtion.getRanges());
        return rootFunction;
    }

//    public RuleFunction getRuleAlgorithm(String name, Map<String, String> properties, Map<String, String> ranges) {
//        Optional<String> clazz = functions.values().stream().filter(i -> name.equals(i.funtion.getName())).findFirst().map(i -> i.getFuntion().getClazz());
//        if (!clazz.isPresent()){
//            throw new IllegalArgumentException("can not found function:"+name);
//        }
//        return getRuleAlgorithm(name,clazz.get(),properties,ranges);
//    }

    public RuleFunction getRuleAlgorithm(String name, String clazz, Map<String, String> properties, Map<String, String> ranges) {
        try {
//            Factory factory = functions.get(name);
//            if (factory != null&&) {
//                factory.get();
//            }
            RuleFunction function = createFunction(name, clazz);
            function.callInit(properties == null ? Collections.emptyMap() : properties, ranges == null ? Collections.emptyMap() : ranges);
            return function;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    public void putRuleAlgorithm(SharingFuntionRootConfig.ShardingFuntion funtion) {
//        functions.put(funtion.getName(), new Factory(funtion));
//    }

    private static class Factory {
        final SharingFuntionRootConfig.ShardingFuntion funtion;

        public Factory(SharingFuntionRootConfig.ShardingFuntion funtion) {
            this.funtion = funtion;
        }

        public RuleFunction get() throws Exception {
            return getRuleAlgorithm(funtion);
        }

        public SharingFuntionRootConfig.ShardingFuntion getFuntion() {
            return funtion;
        }
    }

}