//package io.mycat.client;
//
//import com.alibaba.fastsql.sql.SQLUtils;
//import io.mycat.*;
//import io.mycat.api.collector.RowBaseIterator;
//import io.mycat.api.collector.RowBaseIteratorCacher;
//import io.mycat.beans.mycat.TransactionType;
//import io.mycat.booster.CacheConfig;
//import io.mycat.booster.Task;
//import io.mycat.commands.MycatCommand;
//import io.mycat.commands.MycatdbCommand;
//import io.mycat.hbt4.CacheExecutorImplementor;
//import io.mycat.calcite.DefaultDatasourceFactory;
//import io.mycat.calcite.executor.TempResultSetFactoryImpl;
//import io.mycat.matcher.Matcher;
//import io.mycat.plug.command.MycatCommandLoader;
//import io.mycat.plug.hint.HintLoader;
//import io.mycat.proxy.session.MycatSession;
//import io.mycat.proxy.session.SimpleTransactionSessionRunner;
//import io.mycat.runtime.MycatDataContextImpl;
//import io.mycat.sqlhandler.dml.DrdsRunners;
//import io.mycat.Response;
//import io.mycat.util.StringUtil;
//import lombok.Getter;
//import lombok.SneakyThrows;
//import org.apache.calcite.util.Template;
//import org.jetbrains.annotations.NotNull;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.nio.ByteBuffer;
//import java.nio.CharBuffer;
//import java.nio.charset.StandardCharsets;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author Junwen Chen
// **/
//@Getter
//public class UserSpace {
//    private static final Logger logger = LoggerFactory.getLogger(UserSpace.class);
//    private final String userName;
//    private final Matcher<Map<String, Object>> matcher;
//    private final Map<String, Task> cacheMap = new ConcurrentHashMap<>();
//
//    public UserSpace(String userName, TransactionType defaultTransactionType, Matcher matcher, List<CacheTask> cacheTaskList) {
//        this.userName = Objects.requireNonNull(userName);
//        this.matcher = matcher;
//        ScheduledExecutorService timer = ScheduleUtil.getTimer();
//        cacheTaskList.forEach(config -> {
//            MycatDataContext context = new MycatDataContextImpl(new SimpleTransactionSessionRunner());
//            Task task = getTask(config, context, timer);
//            cacheMap.put(config.getName(), task);
//            task.start();
//        });
//    }
//
//
//    public boolean execute(final ByteBuffer buffer, final MycatSession session) {
//        final CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
//        final Map<String, Object> extractor = new HashMap<>();
//        List<Map<String, Object>> matchList = matcher.match(charBuffer, extractor);
//        if (matchList == null) {
//            matchList = Collections.emptyList();
//        }
//        String text = charBuffer.toString();
//        if (!matchList.isEmpty()){
//            MycatDataContext dataContext = session.getDataContext();
//            int sessionId = session.sessionId();
//            ReceiverImpl receiver = new ReceiverImpl(session, 1, false, false);
//            for (Map<String, Object> item : matchList) {
//                HashMap<String, Object> context = new HashMap<>(item);
//                context.putAll(extractor);
//                if (execute(sessionId, dataContext, text, context, session,receiver)) return true;
//            }
//        }
//      return false;
//    }
//
//    public boolean execute(int sessionId,
//                           MycatDataContext dataContext,
//                           String text,
//                           Map<String, Object> context,
//                           MycatSession session,
//                           Response  receiver) {
//        try {
//            final String name = Objects.requireNonNull((String) context.get("name"), "command is not allowed null");
//            final String command = Objects.requireNonNull((String) context.get("command"), "command is not allowed null");
//
//            //////////////////////////////////hints/////////////////////////////////
//            final boolean cache = !StringUtil.isEmpty((String) context.get("cache"));
//            ///////////////////////////////////cache//////////////////////////////////
//            if (cache) {
//                Optional<RowBaseIterator> mycatResultSetResponse = Optional.ofNullable(cacheMap.get(name)).map(i -> i.get());
//                if (mycatResultSetResponse.isPresent()) {
//                    logger.info("\n" + context + "\n hit cache");
//                    receiver.sendResultSet(mycatResultSetResponse.get());
//                    return true;
//                }
//            }
//            ///////////////////////////////////cache//////////////////////////////////
//            //////////////////////////////////command/////////////////////////////////
//            MycatCommand commandHanlder = MycatCommandLoader.INSTANCE.get(command);
//            if (commandHanlder != null){
//                MycatRequest sqlRequest = new MycatRequest(sessionId, text, context, this);
//                return commandHanlder.run(sqlRequest, dataContext, receiver);
//            }
//            return false;
//            //////////////////////////////////command/////////////////////////////////
//        } catch (Throwable e) {
//            logger.error("", e);
//        }
//        return false;
//    }
//
//
//    //解决获取结果集对象查询和更新的顺序问题,不解决正在写入的结果集回收的问题
//
//    @NotNull
//    public static Task getTask(CacheTask task,
//                               MycatDataContext context,
//                               ScheduledExecutorService timer) {
//        final String text = task.text;
//        Type type = task.type;
//        return new Task(task.cacheConfig) {
//
//            @Override
//            public void start(CacheConfig cacheConfig) {
//                NameableExecutor mycatWorker = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class).getMycatWorker();
//                timer.scheduleAtFixedRate(() -> mycatWorker.execute(() -> {
//                            try {
//                                cache(cacheConfig);
//                            } catch (Exception e) {
//                                logger.error("build cache fail:" + cacheConfig, e);
//                            }
//                        }),
//                        cacheConfig.getInitialDelay().toMillis(),
//                        cacheConfig.getRefreshInterval().toMillis(),
//                        TimeUnit.MILLISECONDS);
//            }
//
//            @Override
//            public synchronized void cache(CacheConfig cacheConfig) {
//                dispatcher(type, text);
//            }
//
//            @SneakyThrows
//            private void dispatcher(Type command, String text) {
//                DefaultDatasourceFactory defaultDatasourceFactory = new DefaultDatasourceFactory(context);
//                TempResultSetFactoryImpl tempResultSetFactory = new TempResultSetFactoryImpl();
//                CacheExecutorImplementor cacheExecutorImplementor = new CacheExecutorImplementor(text, defaultDatasourceFactory, tempResultSetFactory);
//
//                try {
//                    switch (command) {
//                        case SQL: {
//                            DrdsRunners.runOnDrds(context, SQLUtils.parseSingleMysqlStatement(text), cacheExecutorImplementor);
//                            break;
//                        }
//                        case HBT: {
//                            DrdsRunners.runHbtOnDrds(context, text, cacheExecutorImplementor);
//                            break;
//                        }
//                        default:
//                            throw new UnsupportedOperationException(command.toString());
//                    }
//                } catch (Throwable t) {
//                    logger.error("", t);
//                    throw t;
//                }
//            }
//
//            @Override
//            public RowBaseIterator get(CacheConfig cacheConfig) {
//                RowBaseIterator rowBaseIterator = RowBaseIteratorCacher.get(text);
//                if (rowBaseIterator != null) {
//                    return rowBaseIterator;
//                } else {
//                    return null;
//                }
//            }
//        };
//    }
//
//
//}