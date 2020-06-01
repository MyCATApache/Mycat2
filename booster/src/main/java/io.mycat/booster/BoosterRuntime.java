package io.mycat.booster;

import io.mycat.MycatConfig;
import io.mycat.config.PatternRootConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum BoosterRuntime {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(BoosterRuntime.class);
    private MycatConfig config;
    private Map<String, List<String>> boosters = new HashMap<>();

    public synchronized void load(MycatConfig config) {
        if (this.config != config) {
            this.config = config;
            Stream<PatternRootConfig> configStream = Optional.ofNullable(config.getInterceptors())
                    .map(i -> i.stream()).orElse(Stream.empty());
            boosters =
                    configStream.filter(i -> i.getUser() != null)
                            .collect(Collectors
                                    .toMap(k -> k.getUser().getUsername(), v -> v.getBoosters()));
        }
    }

    public Optional<String> getBooster(String name) {
        List<String> strings = boosters.getOrDefault(name, Collections.emptyList());
        if (strings.isEmpty()) {
            return Optional.empty();
        }
        int randomIndex = ThreadLocalRandom.current().nextInt(0, strings.size());
        return Optional.of(strings.get(randomIndex));
    }
}