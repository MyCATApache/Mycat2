package io.mycat.plug.sequence;

import io.mycat.config.PlugRootConfig;
import io.mycat.config.Sequence;
import io.mycat.config.SequenceConfig;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public  class SequenceGenerator {;
    final ConcurrentMap<String, Supplier<String>> map = new ConcurrentHashMap<>();
    private final Logger LOGGER = LoggerFactory.getLogger(SequenceGenerator.class);
    final Supplier<String> defaultSequenceGenerator = new Supplier<String>() {
        @Override
        public String get() {
            LOGGER.error("use default defaultSequenceGenerator may be a error");
            return "9999";
        }
    };

    @SneakyThrows
    public SequenceGenerator(List<SequenceConfig> sequences) {
        if (sequences == null){
            sequences = Collections.emptyList();
        }
        /////////////////////////////////////////SequenceGenerator////////////////////////////////////////////////////////
        register("snowflake", new SequenceSnowflakeGenerator("workerId:1"));
        for (SequenceConfig e : sequences) {
            String name = e.getName();
            String clazz = e.getClazz();
            String args = e.getArgs();
            Class<?> aClass = Class.forName(clazz);
            if (aClass == null) {
                LOGGER.error("{} is not existed", clazz);
                continue;
            }
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructor(String.class);
            Object o = declaredConstructor.newInstance(args);
          register(name, (Supplier) o);
        }

    }

    private void register(String key, Supplier supplier) {
        map.put(key, supplier);
    }

   public Supplier<String> getSequence(String name){
        return map.get(name);
    }
}