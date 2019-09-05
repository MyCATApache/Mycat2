package io.mycat.router.function;

import io.mycat.config.route.ShardingFuntion;
import io.mycat.config.route.SharingFuntionRootConfig;
import io.mycat.config.route.SubShardingFuntion;
import io.mycat.router.RuleAlgorithm;

import java.util.*;
import java.util.function.Supplier;

public enum PartitionRuleAlgorithmManager {
    INSTANCE;
    private final Map<String, Supplier<RuleAlgorithm>> functions = new HashMap<>();
    private final Map<String, String> functionClass = new HashMap<>();

    public void initFunctionsForce(SharingFuntionRootConfig funtions) {
        this.functions.clear();
        initFunctions(funtions);
    }

    public void initFunctions(SharingFuntionRootConfig funtions) {
        if (functions == null || this.functions.isEmpty()) {
            if (funtions.getFunctions() != null) {
                for (ShardingFuntion funtion : funtions.getFunctions()) {
                    ////////////////////////////////////check/////////////////////////////////////////////////
                    Objects.requireNonNull(funtion.getName(), "name of function can not be empty");
                    Objects.requireNonNull(funtion.getClazz(), "clazz of function can not be empty");
                    ////////////////////////////////////check/////////////////////////////////////////////////
                    functionClass.put(funtion.getName(), funtion.getClazz());
                    putRuleAlgorithm(funtion);
                }
            }
        }
    }

    public static RuleAlgorithm createFunction(String name, String clazz)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        Class<?> clz = Class.forName(clazz);
        //判断是否继承AbstractPartitionAlgorithm
        if (!RuleAlgorithm.class.isAssignableFrom(clz)) {
            throw new IllegalArgumentException("rule function must implements "
                    + RuleAlgorithm.class.getName() + ", name=" + name);
        }
        return (RuleAlgorithm) clz.newInstance();
    }

    public RuleAlgorithm getRuleAlgorithm(ShardingFuntion funtion)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Map<String, String> properties = funtion.getProperties();
        properties = (properties == null) ? Collections.emptyMap() : properties;
        funtion.setProperties(properties);
        RuleAlgorithm rootFunction = createFunction(funtion.getName(), funtion.getClazz());
        rootFunction.init(funtion.getProperties(), funtion.getRanges());
        return rootFunction;
    }

    public RuleAlgorithm getRuleAlgorithm(String name) {
        return functions.get(name).get();
    }

    public RuleAlgorithm getRuleAlgorithm(String name, Map<String, String> properties, Map<String, String> ranges) {
        try {
            RuleAlgorithm function = createFunction(name, functionClass.get(name));
            function.init(properties==null?Collections.emptyMap():properties,ranges==null?Collections.emptyMap():ranges);
            return function;
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

    public void putRuleAlgorithm(ShardingFuntion funtion) {
        functions.put(funtion.getName(), () -> {
            try {
                String name = funtion.getName();
                RuleAlgorithm rootFunction = getRuleAlgorithm(funtion);
                ShardingFuntion rootConfig = funtion;
                SubShardingFuntion subFuntionConfig = rootConfig.getSubFuntion();
                if (subFuntionConfig != null) {
                    rootFunction.setSubRuleAlgorithm(getSubRuleAlgorithmList(
                            rootFunction.getPartitionNum(),
                            name,
                            subFuntionConfig));
                }
                return rootFunction;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<RuleAlgorithm> getSubRuleAlgorithmList(int partitionNum, String parent,
                                                        SubShardingFuntion funtion)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Map<String, String> properties = funtion.getProperties();
        Map<String, String> ranges = funtion.getRanges();
        List<RuleAlgorithm> ruleAlgorithms = new ArrayList<>();
        if (properties == null) {
            properties = Collections.EMPTY_MAP;
        }
        if (ranges == null) {
            ranges = Collections.EMPTY_MAP;
        }
        for (int i = 0; i < partitionNum; i++) {
            RuleAlgorithm function = PartitionRuleAlgorithmManager.INSTANCE.createFunction(parent + funtion.toString(), funtion.getClazz());
            function.init(properties, ranges);
            ruleAlgorithms.add(function);
            if (funtion.getSubFuntion() != null) {
                function.setSubRuleAlgorithm(
                        getSubRuleAlgorithmList(function.getPartitionNum(), parent, funtion.getSubFuntion()));
            }
        }
        return ruleAlgorithms;
    }
}