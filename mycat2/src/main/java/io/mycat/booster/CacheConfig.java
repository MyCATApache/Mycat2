package io.mycat.booster;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.mycat.util.SimpleDuration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author Junwen Chen
 **/
@Data
@EqualsAndHashCode
public class CacheConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheConfig.class);
    private SimpleDuration refreshInterval;
    private SimpleDuration initialDelay;
    private static final Splitter KEYS_SPLITTER = Splitter.on(',').trimResults();
    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on('=').trimResults();
    private static final ImmutableMap<String, BiFunction<String, String, Consumer<CacheConfig>>> VALUE_PARSERS =
            ImmutableMap.<String, BiFunction<String, String, Consumer<CacheConfig>>>builder()
                    .put("refreshInterval", (o, o2) -> cacheConfig -> {
                        cacheConfig.setRefreshInterval(SimpleDuration.parse(o, o2));
                    })
                    .put("initialDelay", (s, s2) -> cacheConfig -> cacheConfig.setInitialDelay(SimpleDuration.parse(s, s2)))
                    .build();
    public static CacheConfig create(String cache){
        CacheConfig cacheConfig = new CacheConfig();
        for (String i : KEYS_SPLITTER.split(cache)) {
            ImmutableList<String> strings = ImmutableList.copyOf(KEY_VALUE_SPLITTER.split(i));
            String key = strings.get(0).trim();
            String value = strings.get(1);
            try {
                BiFunction<String, String, Consumer<CacheConfig>> stringStringConsumerBiFunction = VALUE_PARSERS.get(key);
                Consumer<CacheConfig> apply = stringStringConsumerBiFunction.apply(key, value);
                apply.accept(cacheConfig);
            } catch (Exception e) {
                LOGGER.error("",e);
                throw e;
            }
        }
        return cacheConfig;
    }
}