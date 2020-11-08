package io.mycat.plug.sequence;

import io.mycat.config.SequenceConfig;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class SequenceGenerator {

    private final Logger LOGGER = LoggerFactory.getLogger(SequenceGenerator.class);
    private final List<SequenceConfig> sequencesConfig;
    private final ConcurrentHashMap<String, Supplier<Number>> cache = new ConcurrentHashMap<>();


    @SneakyThrows
    public SequenceGenerator(List<SequenceConfig> sequences) {
        if (sequences == null) {
            sequences = new ArrayList<>();
        }
        this.sequencesConfig = sequences;
    }

    @Nullable
    private Supplier<Number> create(String clazz, Map<String,Object> args) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> aClass = Class.forName(clazz);
        if (aClass == null) {
            LOGGER.error("{} is not existed", clazz);
            return null;
        }
        Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(Map.class);
        Supplier<Number> o =(Supplier<Number>) declaredConstructor.newInstance(args);
        return o;
    }

    public Supplier<Number> getSequence(String uniqueName) {
        return cache.computeIfAbsent(uniqueName, new Function<String, Supplier<Number>>() {
            @Override
            @SneakyThrows
            public Supplier<Number> apply(String s) {
                SequenceConfig sequenceConfig1 = sequencesConfig.stream().filter(i -> i.getUniqueName().equals(s)).findFirst().orElseGet(() -> {
                    SequenceConfig sequenceConfig = new SequenceConfig();
                    sequenceConfig.setClazz(SequenceSnowflakeGenerator.class.getCanonicalName());
                    sequenceConfig.setArgs(Collections.singletonMap("workerId", System.getProperty("workerId", "1")));
                    return sequenceConfig;
                });
                return create(sequenceConfig1.getClazz(),sequenceConfig1.getArgs());
            }
        });
    }
}