package io.mycat.boost;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.mycat.MycatConfig;
import io.mycat.RootHelper;
import io.mycat.ScheduleUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.client.ClientRuntime;
import io.mycat.client.Context;
import io.mycat.client.MycatClient;
import io.mycat.config.PatternRootConfig;
import io.mycat.lib.impl.CacheFile;
import io.mycat.lib.impl.CacheLib;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.util.StringUtil;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Log4j
public class UserBooster {
    private static final Splitter KEYS_SPLITTER = Splitter.on(',').trimResults();
    private static final Splitter KEY_VALUE_SPLITTER = Splitter.on('=').trimResults();
    private static final ImmutableMap<String, BiFunction<String, String, Consumer<CacheConfig>>> VALUE_PARSERS =
            ImmutableMap.<String, BiFunction<String, String, Consumer<CacheConfig>>>builder()
                    .put("refreshInterval", (o, o2) -> cacheConfig -> {
                        cacheConfig.setRefreshInterval(Duration.parse(o, o2));
                    })
                    .put("initialDelay", (s, s2) -> cacheConfig -> cacheConfig.setInitialDelay(Duration.parse(s, s2)))
                    .build();
    public static final String DISTRIBUTED_QUERY = "distributedQuery";
    public static final String EXECUTE_PLAN = "executePlan";

    public UserBooster(MycatClient client) {
        List<CacheConfig> cacheConfigs = new ArrayList<>();
        loadConfig(client, cacheConfigs);
        ScheduledExecutorService timer = ScheduleUtil.getTimer();
        for (CacheConfig cacheConfig : cacheConfigs) {
            Task task = getTask(client.getMycatDb(), timer, cacheConfig);
            map.put(cacheConfig.getSqlId(), task);
        }
        map.values().forEach(i -> i.start());
    }

    public static Set<String> getSupportCommands() {
        return ImmutableSet.of(DISTRIBUTED_QUERY, EXECUTE_PLAN);
    }

    final static private ExecutorService executorService = Executors.newCachedThreadPool();
    final private ConcurrentHashMap<Integer, Task> map = new ConcurrentHashMap<>();
    final private static ConcurrentHashMap<String, UserBooster> userMap = new ConcurrentHashMap<>();

    public static MycatResultSetResponse getResultSetBySqlId(String username, Integer sql) {
        return Optional.ofNullable(userMap.get(username)).map(i -> i.map.get(sql)).map(i -> (MycatResultSetResponse) i.get()).orElse(null);
    }

    public static void init() {
        for (MycatClient client : ClientRuntime.INSTANCE.getDefaultUsers()) {
            UserBooster userBooster = new UserBooster(client);
            userMap.put(client.getUser().getUserName(),userBooster);
        }
    }

    @NotNull
    private UserBooster.Task getTask(MycatDBClientMediator db, ScheduledExecutorService timer, CacheConfig cacheConfig) {
        return new Task(cacheConfig) {
            volatile CacheFile cache;

            @Override
            void start(CacheConfig cacheConfig) {

                timer.scheduleAtFixedRate(() -> executorService.execute(() -> cache(cacheConfig)),
                        cacheConfig.getInitialDelay().toMillis(),
                        cacheConfig.getRefreshInterval().toMillis(),
                        TimeUnit.MILLISECONDS);
            }

            @Override
            synchronized void cache(CacheConfig cacheConfig) {
                String command = cacheConfig.getCommand();
                String text = cacheConfig.getText();
                RowBaseIterator query = dispatcher(command, text);
                CacheFile cache2 = cache;

                text = text.replaceAll("[\\?\\\\/:|<>\\*]", " "); //filter ? \ / : | < > *
                text = text.replaceAll("\\s+", "_");
                cache = CacheLib.cache(() -> new TextResultSetResponse(query), text.replaceAll(" ", "_"));
                if (cache2 != null) {
                    cache2.close();
                }
            }

            private RowBaseIterator dispatcher(String command, String text) {
                RowBaseIterator query;
                switch (command) {
                    case DISTRIBUTED_QUERY: {
                        try {
                            query = db.query(text);
                        } finally {
                            db.recycleResource();
                        }
                        break;
                    }
                    case EXECUTE_PLAN: {
                        try {
                            query = db.executeRel(text);
                        } finally {
                            db.recycleResource();
                        }
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException(command);
                }
                return query;
            }

            @Override
            MycatResultSetResponse get(CacheConfig cacheConfig) {
                if (cache != null) {
                    return cache.cacheResponse();
                } else return null;
            }
        };
    }


    //解决获取结果集对象查询和更新的顺序问题,不解决正在写入的结果集回收的问题
    @AllArgsConstructor
    static abstract class Task {
        final CacheConfig cacheConfig;

        public CacheConfig config() {
            return cacheConfig;
        }

        void start() {
            start(cacheConfig);
        }

        abstract void start(CacheConfig cacheConfig);

        void cache() {
            cache(cacheConfig);
        }

        abstract void cache(CacheConfig cacheConfig);

        <T> T get() {
            return (T) get(cacheConfig);
        }

        abstract <T> T get(CacheConfig cacheConfig);
    }


    private static void loadConfig(MycatClient client, List<CacheConfig> cacheConfigs) {
        MycatConfig config = RootHelper.INSTANCE.getConfigProvider().currentConfig();
        String username = client.getUser().getUserName();
        config.getInterceptors().stream().filter(i -> username.equals(i.getUser().getUsername())).findFirst().ifPresent(patternRootConfig -> {
            Stream<PatternRootConfig.TextItemConfig> stream1 = patternRootConfig.getSchemas().stream()
                    .flatMap(i -> i.getSqls().stream()).filter(i -> !StringUtil.isEmpty(i.getCache()));
            for (PatternRootConfig.TextItemConfig textItemConfig : Stream
                    .concat(stream1, patternRootConfig.getSqls().stream()
                            .filter(i -> !StringUtil.isEmpty(i.getCache())))
                    .collect(Collectors.toList())) {
                String cache = textItemConfig.getCache();
                String command = textItemConfig.getCommand();
                CacheConfig cacheConfig = new CacheConfig();
                cacheConfig.setCommand(command);

                switch (command) {
                    case DISTRIBUTED_QUERY: {
                        cacheConfig.setText(textItemConfig.getSql());
                        break;
                    }
                    case EXECUTE_PLAN: {
                        cacheConfig.setText(textItemConfig.getExplain());
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException(command);
                }
                Context analysis = client.analysis(cacheConfig.getText());
                cacheConfig.setSqlId(Objects.requireNonNull(analysis.getSqlId(), textItemConfig + " " + "sql id is null"));

                for (String i : KEYS_SPLITTER.split(cache)) {
                    ImmutableList<String> strings = ImmutableList.copyOf(KEY_VALUE_SPLITTER.split(i));
                    String key = strings.get(0).trim();
                    String value = strings.get(1);
                    try {
                        BiFunction<String, String, Consumer<CacheConfig>> stringStringConsumerBiFunction = VALUE_PARSERS.get(key);
                        Consumer<CacheConfig> apply = stringStringConsumerBiFunction.apply(key, value);
                        apply.accept(cacheConfig);
                    } catch (Exception e) {
                        log.error(e);
                    }
                }
                cacheConfigs.add(cacheConfig);
            }
        });

    }
}