package io.mycat.boost;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.mycat.MycatConfig;
import io.mycat.RootHelper;
import io.mycat.ScheduleUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.config.PatternRootConfig;
import io.mycat.lib.impl.CacheFile;
import io.mycat.lib.impl.CacheLib;
import io.mycat.proxy.session.SimpleTransactionSessionRunner;
import io.mycat.resultset.TextResultSetResponse;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.StringUtil;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
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
                    .put("refreshInterval", (o, o2) -> cacheConfig -> {
                        cacheConfig.setRefreshInterval(Duration.parse(o, o2));
                    })
                    .put("initialDelay", (s, s2) -> cacheConfig -> cacheConfig.setInitialDelay(Duration.parse(s, s2)))
                    .build();
    public static final String DISTRIBUTED_QUERY = "distributedQuery";
    public static final String EXECUTE_PLAN = "executePlan";
    final private ExecutorService executorService = Executors.newCachedThreadPool();
    final private ConcurrentHashMap<String, Task> map = new ConcurrentHashMap<>();


    public MycatResultSetResponse getResultSetBySql(String sql){
        Task task = map.get(sql);
        if (task!=null){
            return task.get();
        }
        return null;
    }

    public void init() {
        map.clear();

        MycatDBClientMediator client = MycatDBs.createClient(new MycatDataContextImpl(new SimpleTransactionSessionRunner()));
        List<CacheConfig> cacheConfigs = new ArrayList<>();
        loadConfig(cacheConfigs);

        ScheduledExecutorService timer = ScheduleUtil.getTimer();

        for (CacheConfig cacheConfig : cacheConfigs) {
            Task task = new Task(cacheConfig) {
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
                    cache = CacheLib.cache(() -> new TextResultSetResponse(query), text.replaceAll(" ","_"));
                    if (cache2!=null) {
                        cache2.close();
                    }
                }

                private RowBaseIterator dispatcher(String command, String text) {
                    RowBaseIterator query;
                    switch (command) {
                        case DISTRIBUTED_QUERY: {
                            query = client.query(text);
                            break;
                        }
                        case EXECUTE_PLAN: {
                            query = client.executeRel(text);
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException(command);
                    }
                    return query;
                }

                @Override
                MycatResultSetResponse get(CacheConfig cacheConfig) {
                    return cache.cacheResponse();
                }
            };
            map.put(cacheConfig.getText(), task);
        }
        map.values().forEach(i -> i.start());
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


    private static void loadConfig(List<CacheConfig> cacheConfigs) {
        MycatConfig config = RootHelper.INSTANCE.getConfigProvider().currentConfig();
        Stream<PatternRootConfig.TextItemConfig> stream1 = config.getInterceptor().getSchemas().stream().flatMap(i -> i.getSqls().stream());
        for (PatternRootConfig.TextItemConfig textItemConfig : Stream
                .concat(stream1, config.getInterceptor().getSqls().stream())
                .filter(i -> !StringUtil.isEmpty(i.getCache())).collect(Collectors.toList())) {
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
    }
}