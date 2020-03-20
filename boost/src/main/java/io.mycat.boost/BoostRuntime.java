package io.mycat.boost;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.mycat.MycatConfig;
import io.mycat.RootHelper;
import io.mycat.config.PatternRootConfig;
import io.mycat.util.StringUtil;
import lombok.extern.log4j.Log4j;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
public enum BoostRuntime {
    INSTANCE;
    private static final Splitter KEYS_SPLITTER = Splitter.on(',').trimResults();
    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on('=').trimResults();
    private static final ImmutableMap<String, BiFunction<String, String, Consumer<CacheConfig>>> VALUE_PARSERS =
            ImmutableMap.<String, BiFunction<String, String, Consumer<CacheConfig>>>builder()
                    .put("refreshInterval", (o, o2) -> cacheConfig -> cacheConfig.setRefreshInterval(Duration.parse(o, o2)))
                    .put("initialDelay", (s, s2) -> cacheConfig -> cacheConfig.setInitialDelay(Duration.parse(s, s2)))
                    .build();

    public static void main(String[] args) {
//        List<CacheConfig> cacheConfigs = new ArrayList<>();
//        loadConfig(cacheConfigs);
//        ConcurrentHashMap<String, Task> map = new ConcurrentHashMap<>();
//        LinkedList<Task> lazyQueue = new LinkedList<>();
//        ScheduledExecutorService timer = ScheduleUtil.getTimer();
//        for (CacheConfig cacheConfig : cacheConfigs) {
//            String sql = cacheConfig.getSql();
//            Task o = map.get(sql);
//            if (o != null && o.config().equals(cacheConfig)) {
//                continue;
//            }
//            if (o != null && !o.config().equals(cacheConfig)) {
//                lazyQueue.add(o);
//                continue;
//            }
//            if (o == null) {
//                map.put(sql, new Task() {
//                    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
//                    final Object attr;
//
//                    @Override
//                    public CacheConfig config() {
//                        return cacheConfig;
//                    }
//
//                    @Override
//                    public boolean takeRead() {
//                        return lock.readLock().tryLock();
//                    }
//
//                    @Override
//                    public boolean takeWrite() {
//                        return lock.writeLock().tryLock();
//                    }
//
//                    @Override
//                    public synchronized void close() {
//                        if (!lock.isWriteLocked()) {
//                            throw new AssertionError();
//                        }
//                    }
//                });
//            }
//
//        }

    }

//    @AllArgsConstructor
//    static abstract class Task {
//        final CacheConfig cacheConfig;
//
//        public CacheConfig config() {
//            return cacheConfig;
//        }
//
//        boolean takeRead(){
//
//        }
//
//        boolean takeWrite();
//
//        void close();
//
//
//    }

    private static void loadConfig(List<CacheConfig> cacheConfigs) {
        MycatConfig config = RootHelper.INSTANCE.getConfigProvider().currentConfig();
        Stream<PatternRootConfig.TextItemConfig> stream1 = config.getInterceptor().getSchemas().stream().flatMap(i -> i.getSqls().stream());
        for (PatternRootConfig.TextItemConfig textItemConfig : Stream
                .concat(stream1, config.getInterceptor().getSqls().stream())
                .filter(i -> !StringUtil.isEmpty(i.getCache())).collect(Collectors.toList())) {
            String sql = textItemConfig.getSql();
            String cache = textItemConfig.getCache();
            CacheConfig cacheConfig = new CacheConfig();
            cacheConfig.setSql(sql);
            for (String i : KEYS_SPLITTER.split(cache)) {
                ImmutableList<String> strings = ImmutableList.copyOf(KEY_VALUE_SPLITTER.split(i));
                String key = strings.get(0).toLowerCase().trim();
                String value = strings.get(1);
                try {
                    VALUE_PARSERS.get(key).apply(key, value).accept(cacheConfig);
                } catch (Exception e) {
                    log.error(e);
                }
            }
            cacheConfigs.add(cacheConfig);
        }
    }
}