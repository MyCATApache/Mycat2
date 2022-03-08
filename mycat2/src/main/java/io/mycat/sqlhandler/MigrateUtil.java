package io.mycat.sqlhandler;

import com.alibaba.druid.util.JdbcUtils;
import groovy.transform.ToString;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatException;
import io.mycat.Partition;
import io.mycat.ScheduleUtil;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.config.DatasourceConfig;
import io.mycat.config.MycatRouterConfig;
import io.mycat.replica.ReplicaSelectorManager;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Cancellable;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.mycat.vertxmycat.JdbcMySqlConnection.setStreamFlag;

public class MigrateUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateUtil.class);
    public static CopyOnWriteArrayList<MigrateScheduler> schedulers = new CopyOnWriteArrayList();
    public final static AtomicInteger IDS = new AtomicInteger();

    public static RowBaseIterator list() {
        List<MigrateScheduler> schedulers = MigrateUtil.schedulers;
        return show(schedulers);
    }

    public static RowBaseIterator show(MigrateScheduler scheduler) {
        return show(Collections.singletonList(scheduler));
    }

    public static RowBaseIterator show(List<MigrateScheduler> schedulers) {
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("ID", JDBCType.VARCHAR);
        builder.addColumnInfo("NAME", JDBCType.VARCHAR);
        builder.addColumnInfo("PROCESS", JDBCType.VARCHAR);
        builder.addColumnInfo("COMPLETE", JDBCType.INTEGER);
        builder.addColumnInfo("INFO", JDBCType.VARCHAR);
        builder.addColumnInfo("ERROR", JDBCType.VARCHAR);
        builder.addColumnInfo("START_TIME", JDBCType.TIMESTAMP);
        builder.addColumnInfo("END_TIME", JDBCType.TIMESTAMP);
        builder.addColumnInfo("INPUT_ROW", JDBCType.BIGINT);
        builder.addColumnInfo("OUTPUT_ROW", JDBCType.BIGINT);
        for (MigrateScheduler scheduler : schedulers) {
            String id = scheduler.getId();
            String name = scheduler.getName();
            int complete = scheduler.getFuture().isComplete() ? 1 : 0;
            String process = scheduler.computeProcess() * 100 + "%";
            String info = scheduler.toString();
            builder.addObjectRowPayload(
                    new Object[]{
                            id, name, process, complete, info, scheduler.getFuture().cause(),
                            scheduler.getStartTime(), scheduler.getEndTime(),
                            scheduler.computeInputRow(),
                            scheduler.getOutput().getRow().get()
                    }
            );
        }
        return builder.build();
    }

    public static boolean stop(String id) {
        Optional<MigrateScheduler> optional = schedulers.stream().filter(i -> id.equals(id)).findFirst();
        optional.ifPresent(
                new Consumer<MigrateScheduler>() {
                    @Override
                    public void accept(MigrateScheduler migrateScheduler) {
                        migrateScheduler.stop();
                    }
                }
        );
        return optional.isPresent();
    }

    public static MigrateScheduler register(String name,
                                            List<MigrateJdbcInput> inputs,
                                            MigrateJdbcOutput output,
                                            MigrateController controller) {
        MigrateScheduler scheduler = MigrateScheduler.of(name, inputs, output, controller);
        schedulers.add(scheduler);
        controller.getFuture().onSuccess(event -> {
            RowBaseIterator list = list();
            LOGGER.info("----------------------------Migration-INFO-----------------------------------------------");
            List<Map<String, Object>> resultSetMap = list.getResultSetMap();
            for (Map<String, Object> stringObjectMap : resultSetMap) {
                LOGGER.info(stringObjectMap.toString());
            }
            LOGGER.info("-----------------------------------------------------------------------------------------");
            ScheduleUtil.getTimerFuture(() -> {
                schedulers.remove(scheduler);
                LOGGER.info("----------------------------Migration-REMOVE-{}---------------------------------------",
                        scheduler.getName());
            }, 1, TimeUnit.HOURS);
        });
        return scheduler;
    }

    public static MigrateController writeSql(MigrateJdbcAnyOutput output, Flowable<BinlogUtil.ParamSQL> merge) {
        String username = output.getUsername();
        String password = output.getPassword();
        String url = output.getUrl();

        @ToString
        @Getter
        class MigrateSQLController implements MigrateController, FlowableSubscriber<BinlogUtil.ParamSQL> {
            Subscription subscription;
            Connection connection;
            final Promise<Void> promise = Promise.promise();
            final MigrateJdbcAnyOutput info = output;

            @Override
            public void onSubscribe(@NonNull Subscription subscription) {
                try {
                    this.subscription = subscription;
                    this.connection = DriverManager.getConnection(url, username, password);
                    this.subscription.request(1);
                } catch (Throwable exception) {
                    onError(exception);
                }
            }

            @Override
            public void onNext(BinlogUtil.ParamSQL paramSQL) {
                try {
                    JdbcUtils.execute(this.connection, paramSQL.getSql(), (paramSQL.getParams()));
                    this.subscription.request(1);
                } catch (Throwable exception) {
                    onError(exception);
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.error("MigrateSQLController error", t);
                stop();
            }

            @Override
            public void onComplete() {
                stop();
            }

            @Override
            public Future<Void> getFuture() {
                return promise.future();
            }

            @Override
            public void stop() {
                LOGGER.error("MigrateSQLController stop");
                if (this.subscription != null) {
                    this.subscription.cancel();
                }
                if (this.connection != null) {
                    JdbcUtils.close(this.connection);
                }
                promise.tryComplete();
            }
        };
        MigrateSQLController migrateSQLController = new MigrateSQLController();
        merge.subscribe(migrateSQLController);
        return migrateSQLController;
    }

    @Data
    @ToString
    public static class MigrateJdbcInput {
        long count;
        final AtomicLong row = new AtomicLong();
    }

    @Data
    @ToString
    public static class MigrateJdbcOutput {
        String username;
        String password;
        String url;
        String insertTemplate;
        int parallelism = 1;
        int batch = 1000;
        final AtomicLong row = new AtomicLong();
    }

    @Data
    @ToString
    public static class MigrateJdbcAnyOutput {
        String username;
        String password;
        String url;
    }

    @Getter
    @ToString
    public static class MigrateScheduler {
        String id;
        String name;
        List<MigrateJdbcInput> inputs;
        MigrateJdbcOutput output;
        Future<Void> future;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime;
        private MigrateController controller;

        public MigrateScheduler(String name,
                                List<MigrateJdbcInput> inputs,
                                MigrateJdbcOutput output,
                                MigrateController controller) {
            this.controller = controller;
            this.id = UUID.randomUUID().toString();
            this.name = name;
            this.inputs = inputs;
            this.output = output;
            this.future = this.controller.getFuture();

            future.onComplete(event -> MigrateScheduler.this.endTime = LocalDateTime.now());
        }

        public double computeProcess() {
            long total = computeInputRow();
            long nowOutputRow = output.getRow().get();
            if (total == nowOutputRow) {
                return 1;
            }
            return nowOutputRow * 1.0 / total;
        }

        public long computeInputRow() {
            return inputs.stream().mapToLong(i -> i.getCount()).sum();
        }

        public static MigrateScheduler of(String name,
                                          List<MigrateJdbcInput> inputs,
                                          MigrateJdbcOutput output,
                                          MigrateController controller) {
            return new MigrateScheduler(name, inputs, output, controller);
        }

        @Override
        public String toString() {
            return "MigrateScheduler{" +
                    "name='" + name + '\'' +
                    ", inputs=" + inputs +
                    ", output=" + output +
                    ", future=" + future +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    '}';
        }

        public void stop() {
            controller.stop();
        }
    }

    @SneakyThrows
    public static Flowable<Object[]> read(MigrateUtil.MigrateJdbcInput migrateJdbcInput, Partition backend) {
        MycatRouterConfig routerConfig = MetaClusterCurrent.wrapper(MycatRouterConfig.class);
        ReplicaSelectorManager replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);

        String targetName = backend.getTargetName();
        String tableName = backend.getTable();
        String schemaName = backend.getSchema();

        String datasourceName = replicaSelectorRuntime.getDatasourceNameByReplicaName(targetName, true, null);

        List<DatasourceConfig> datasources = routerConfig.getDatasources();
        DatasourceConfig datasourceConfig = datasources.stream().filter(i -> i.getName().equals(datasourceName)).findFirst().orElseThrow((Supplier<Throwable>) () -> {
            MycatException mycatException = new MycatException("can not found datasource " + datasourceName);
            LOGGER.error("", mycatException);
            return mycatException;
        });

        return read(migrateJdbcInput, tableName, schemaName, datasourceConfig.getUrl(), datasourceConfig.getUser(), datasourceConfig.getPassword());
    }

    @SneakyThrows
    public static Flowable<Object[]> read(MigrateJdbcInput migrateJdbcInput, String tableName, String schemaName, String url, String user, String password) {
        String queryCountSql = "select count(1) from `" + schemaName + "`.`" + tableName + "`";
        try (Connection connection = DriverManager.getConnection(url, user, password);) {
            Number countO = (Number) JdbcUtils.executeQuery(connection, queryCountSql, Collections.emptyList()).get(0).values().iterator().next();
            migrateJdbcInput.setCount(countO.longValue());
        }
        String querySql = "select * from `" + schemaName + "`.`" + tableName + "`";
        return read(migrateJdbcInput, url, user, password, querySql);
    }

    public static Flowable<Object[]> read(MigrateJdbcInput migrateJdbcInput, String url, String user, String password, String querySql) {
        return Flowable.create(new FlowableOnSubscribe<Object[]>() {
            @Override
            public void subscribe(@NonNull FlowableEmitter<Object[]> emitter) throws Throwable {
                LOGGER.info("read resultset for:{}, thread:{}", migrateJdbcInput, Thread.currentThread());
                try (Connection connection = DriverManager.getConnection(url, user, password);) {
                    emitter.setCancellable(new Cancellable() {
                        @Override
                        public void cancel() throws Throwable {
                            connection.close();
                            LOGGER.info("close " + migrateJdbcInput);
                        }
                    });
                    Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    setStreamFlag(statement);
                    ResultSet resultSet = statement.executeQuery(querySql);
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (resultSet.next() && !emitter.isCancelled()) {

                        while (emitter.requested() < 1) {
                            LOGGER.info("wait request {}", migrateJdbcInput);
                            TimeUnit.SECONDS.sleep(1);
                            if (emitter.isCancelled()) {
                                LOGGER.info("cancel in wait request {}", migrateJdbcInput);
                                return;
                            }
                        }
                        migrateJdbcInput.getRow().getAndIncrement();
                        Object[] objects = new Object[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            objects[i] = resultSet.getObject(i + 1);
                        }
                        emitter.onNext(objects);
                    }
                    emitter.onComplete();
                } catch (Throwable throwable) {
                    emitter.onError(throwable);
                }
            }
        }, BackpressureStrategy.BUFFER);
    }

    public interface MigrateController {

        public Future<Void> getFuture();

        public void stop();

    }

    @ToString
    public static class MigrateControllerImpl implements MigrateController, Observer<List<Object[]>>, FlowableSubscriber<List<Object[]>> {

        Connection connection;
        Disposable disposable;
        Promise<Void> promise = Promise.promise();

        MigrateJdbcOutput output;
        Subscription subscription;

        public MigrateControllerImpl(MigrateJdbcOutput output) {
            this.output = output;
        }

        public Future<Void> getFuture() {
            return promise.future();
        }

        public void stop() {
            close();
            promise.tryComplete();
        }

        private void close() {
            if (subscription != null) {
                subscription.cancel();
            }
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
            }
            if (connection != null) {
                JdbcUtils.close(connection);
            }
        }


        @Override
        public void onSubscribe(@NonNull Disposable d) {
            this.disposable = d;
        }

        @Override
        public void onNext(@NonNull List<Object[]> objects) {
            try {
                if (connection == null) {
                    LOGGER.info("create output connection for {} thread:{}", MigrateControllerImpl.this.output, Thread.currentThread());
                    connection = DriverManager.getConnection(MigrateControllerImpl.this.output.getUrl(),
                            MigrateControllerImpl.this.output.getUsername(),
                            MigrateControllerImpl.this.output.getPassword());
                }
                PreparedStatement preparedStatement = connection.prepareStatement(this.output.getInsertTemplate());
                for (Object[] object : objects) {
                    int i = 1;
                    for (Object o : object) {
                        preparedStatement.setObject(i, o);
                        i++;
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                preparedStatement.clearParameters();
                output.row.addAndGet(objects.size());
                if (this.subscription != null) {
                    this.subscription.request(1);
                }
            } catch (Exception e) {
                onError(e);
            }
        }

        @Override
        public void onError(@NonNull Throwable e) {
            close();
            promise.tryFail(e);
        }

        @Override
        public void onComplete() {
            close();
            promise.tryComplete();
        }

        @Override
        public void onSubscribe(@NonNull Subscription subscription) {
            this.subscription = subscription;
            this.subscription.request(1);
        }
    }

    @ToString
    public static class MigrateControllerGroup implements MigrateController {
        final List<MigrateController> list;

        public MigrateControllerGroup(List<MigrateController> list) {
            this.list = list;
        }

        public static MigrateController of(List<MigrateController> list) {
            return new MigrateControllerGroup(list);
        }

        @Override
        public Future<Void> getFuture() {
            List<Future> futures = list.stream().map(i -> i.getFuture()).collect(Collectors.toList());
            return CompositeFuture.join(futures).mapEmpty();
        }

        @Override
        public void stop() {
            for (MigrateController migrateController : list) {
                migrateController.stop();
            }
        }
    }

    public static MigrateController write(MigrateJdbcOutput output, Flowable<List<Object[]>> concat) {
        if (output.getParallelism() < 2) {
            MigrateControllerImpl migrateController = new MigrateControllerImpl(output);
            concat.subscribe(migrateController);
            return migrateController;
        }
        @NonNull ParallelFlowable<List<Object[]>> buffer = concat.parallel(output.getParallelism());
        int parallelism = output.getParallelism();
        List<MigrateControllerImpl> list = IntStream.range(0, parallelism).mapToObj(i -> new MigrateControllerImpl(output)).collect(Collectors.toList());
        buffer.runOn(Schedulers.io()).subscribe(list.stream().toArray((IntFunction<Subscriber<? super List<Object[]>>[]>) value -> new Subscriber[value]));
        return MigrateControllerGroup.of((List) list);
    }

}
