package io.mycat.boost;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.util.Duration;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.function.BiFunction;
import java.util.function.Consumer;

@Data
@EqualsAndHashCode
public class CacheConfig {
    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(CacheConfig.class);
    private Duration refreshInterval;
    private Duration initialDelay;
    private static final Splitter KEYS_SPLITTER = Splitter.on(',').trimResults();
    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on('=').trimResults();
    private static final ImmutableMap<String, BiFunction<String, String, Consumer<CacheConfig>>> VALUE_PARSERS =
            ImmutableMap.<String, BiFunction<String, String, Consumer<CacheConfig>>>builder()
                    .put("refreshInterval", (o, o2) -> cacheConfig -> {
                        cacheConfig.setRefreshInterval(Duration.parse(o, o2));
                    })
                    .put("initialDelay", (s, s2) -> cacheConfig -> cacheConfig.setInitialDelay(Duration.parse(s, s2)))
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