package io.mycat.plug.sequence;

import io.mycat.MetaClusterCurrent;
import io.mycat.config.SequenceConfig;
import io.mycat.config.ServerConfig;
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
import java.util.function.Function;
import java.util.function.Supplier;

public class SequenceGenerator {

    private final Logger LOGGER = LoggerFactory.getLogger(SequenceGenerator.class);
    private final List<SequenceConfig> sequencesConfig;
    private final ConcurrentHashMap<String, SequenceHandler> cache = new ConcurrentHashMap<>();
    private final long workerId;


    @SneakyThrows
    public SequenceGenerator(long workerId, List<SequenceConfig> sequences) {
        this.workerId = workerId;
        if (sequences == null) {
            sequences = new ArrayList<>();
        }
        this.sequencesConfig = sequences;
    }

    @Nullable
    private Supplier<Number> create(String clazz, Map<String, Object> args) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> aClass = Class.forName(clazz);
        if (aClass == null) {
            LOGGER.error("{} is not existed", clazz);
            return null;
        }
        Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(Map.class);
        Supplier<Number> o = (Supplier<Number>) declaredConstructor.newInstance(args);
        return o;
    }

    public SequenceHandler getSequence(String uniqueName) {
        return cache.computeIfAbsent(uniqueName, new Function<String, SequenceHandler>() {
            @Override
            @SneakyThrows
            public SequenceHandler apply(String s) {
                SequenceConfig sequenceConfig1 = sequencesConfig.stream()
                        .filter(i -> i.getName().equals(s)).findFirst().orElseGet(() -> {
                            SequenceConfig sequenceConfig = new SequenceConfig();
                            sequenceConfig.setTime(true);
                            sequenceConfig.setName(uniqueName);
                            return sequenceConfig;
                        });
                if (sequenceConfig1.isTime()) {
                    TimeBasedSequence timeBasedSequence = new TimeBasedSequence();
                    timeBasedSequence.init(sequenceConfig1, workerId);
                }
                String clazz = sequenceConfig1.getClazz();
                Class<?> aClass = Class.forName(clazz);
                SequenceHandler o = (SequenceHandler)aClass.newInstance();
                o.init(sequenceConfig1,workerId);
                return o;
            }
        });
    }
}