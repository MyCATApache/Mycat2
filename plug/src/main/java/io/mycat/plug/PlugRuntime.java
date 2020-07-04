package io.mycat.plug;

import io.mycat.Hint;
import io.mycat.MycatConfig;
import io.mycat.config.PlugRootConfig;
import io.mycat.plug.command.MycatCommandLoader;
import io.mycat.plug.hint.HintLoader;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.plug.sequence.SequenceSnowflakeGenerator;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Supplier;

public enum PlugRuntime {
    INSTANCE;
    volatile LoadBalanceManager manager;
    volatile MycatConfig mycatConfig;
    static final Logger LOGGER = LoggerFactory.getLogger(PlugRuntime.class);

    PlugRuntime() {

    }

    @SneakyThrows
    public void load(MycatConfig mycatConfig) {
        if (this.mycatConfig == null || this.mycatConfig != mycatConfig) {
            PlugRootConfig plugRootConfig = mycatConfig.getPlug();
            Objects.requireNonNull(plugRootConfig, "plug config can not found");
            LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
            loadBalanceManager.load(mycatConfig.getPlug().getLoadBalance());
            this.manager = loadBalanceManager;


            /////////////////////////////////////////SequenceGenerator////////////////////////////////////////////////////////
            SequenceGenerator.INSTANCE.register("snowflake", new SequenceSnowflakeGenerator("workerId:1"));
            for (PlugRootConfig.SequenceConfig sequence : Optional.ofNullable(plugRootConfig).map(i -> i.getSequence()).map(i -> i.getSequences()).orElse(Collections.emptyList())) {
                String name = sequence.getName();
                String clazz = sequence.getClazz();
                String args = sequence.getArgs();
                Class<?> aClass = Class.forName(clazz);
                if (aClass == null) {
                    LOGGER.error("{} is not existed", clazz);
                    continue;
                }
                Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class);
                Object o = declaredConstructor.newInstance(args);
                SequenceGenerator.INSTANCE.register(name, (Supplier) o);
            }

            /////////////////////////////////////////hint////////////////////////////////////////////////////////
            for (PlugRootConfig.HintConfig hintConfig : Optional.ofNullable(plugRootConfig).map(i -> i.getHint()).map(i -> i.getHints()).orElse(Collections.emptyList())) {
                String name = hintConfig.getName();
                String clazz = hintConfig.getClazz();
                String args = hintConfig.getArgs();
                Class<?> aClass = Class.forName(clazz);
                if (aClass == null) {
                    LOGGER.error("{} is not existed", clazz);
                    continue;
                }
                Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class);
                Object o = declaredConstructor.newInstance(args);
                HintLoader.INSTANCE.register(name, (Hint) o);
            }
            /////////////////////////////////////////MycatCommand////////////////////////////////////////////////////////
            List<PlugRootConfig.MycatCommandConfig> mycatCommandConfigs = new ArrayList<>();
            mycatCommandConfigs.addAll(Optional.ofNullable(plugRootConfig).map(i -> i.getCommand()).map(i -> i.getCommands()).orElse(Collections.emptyList()));

            for (PlugRootConfig.MycatCommandConfig commandConfig : mycatCommandConfigs) {
                String name = commandConfig.getName();
                String clazz = commandConfig.getClazz();
                MycatCommandLoader.INSTANCE.register(name, getMycatCommand(clazz));
            }
        }
    }

    public static Object getMycatCommand(String clazz)
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Object o = null;
        Class<?> aClass = Class.forName(clazz);
        Method method = aClass.getMethod("values");
        Object[] invoke = (Object[]) method.invoke(null);
        o = invoke[0];
        return o;
    }

    public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
        return manager.getLoadBalanceByBalanceName(name);
    }

    public LoadBalanceManager getManager() {
        return Objects.requireNonNull(manager);
    }
}