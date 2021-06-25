package io.mycat;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import hu.akarnokd.rxjava3.operators.Flowables;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.*;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.*;
import io.mycat.calcite.spm.ParamHolder;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.calcite.table.NormalTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.util.VertxUtil;
import io.mycat.vertx.VertxExecuter;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.sqlclient.SqlConnection;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public abstract class AsyncMycatDataContextImpl extends NewMycatDataContextImpl {
    final Map<String, Future<SqlConnection>> transactionConnnectionMap = new HashMap<>();// int transaction
    final List<Future<SqlConnection>> connnectionFutureCollection = new LinkedList<>();//not int transaction
    final Map<String, List<Observable<Object[]>>> shareObservable = new HashMap<>();

    public AsyncMycatDataContextImpl(MycatDataContext dataContext,
                                     CodeExecuterContext context,
                                     DrdsSqlWithParams drdsSqlWithParams) {
        super(dataContext, context, drdsSqlWithParams);
    }

    Future<SqlConnection> getConnection(String key) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        if (context.isInTransaction()) {
            return transactionConnnectionMap
                    .computeIfAbsent(key, s -> transactionSession.getConnection(key));
        }
        MySQLManager mySQLManager = MetaClusterCurrent.wrapper(MySQLManager.class);
        Future<SqlConnection> connection = mySQLManager.getConnection(key);
        connnectionFutureCollection.add(connection);
        return connection;
    }

    void recycleConnection(String key, Future<SqlConnection> connectionFuture) {
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        if (context.isInTransaction()) {
            transactionConnnectionMap.put(key, connectionFuture);
            return;
        }
        connectionFuture = connectionFuture.flatMap(c -> c.close().mapEmpty());
        transactionSession.addCloseFuture(connectionFuture.mapEmpty());
        connnectionFutureCollection.add(connectionFuture);
    }

    @NotNull
    public synchronized List<Observable<Object[]>> getObservables(ImmutableMultimap<String, SqlString> expand, MycatRowMetaData calciteRowMetaData) {
        LinkedList<Observable<Object[]>> observables = new LinkedList<>();
        for (Map.Entry<String, SqlString> entry : expand.entries()) {
            String key = context.resolveDatasourceTargetName(entry.getKey());
            SqlString sqlString = entry.getValue();
            Observable<Object[]> observable = Observable.create(emitter -> {
                Future<SqlConnection> sessionConnection = getConnection(key);
                PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
                Observable<Object[]> innerObservable = Objects.requireNonNull(VertxExecuter.runQuery(sessionConnection,
                        sqlString.getSql(),
                        MycatPreparedStatementUtil.extractParams(drdsSqlWithParams.getParams(), sqlString.getDynamicParameters()), calciteRowMetaData));
                innerObservable.subscribe(objects -> {
                            emitter.onNext((objects));
                        },
                        throwable -> {
                            sessionConnection.onSuccess(c -> {
                                //close connection?
                                promise.fail(throwable);
                            })
                                    .onFailure(t -> promise.fail(t));
                        }, () -> {
                            sessionConnection.onSuccess(c -> {
                                promise.tryComplete(c);
                            }).onFailure(t -> promise.fail(t));
                            ;
                        });
                recycleConnection(key,
                        promise.future()
                                .onSuccess(c -> {
                                    emitter.onComplete();
                                })
                                .onFailure(t -> {
                                    emitter.onError(t);
                                }));
            });
            observables.add(observable);
        }
        return observables;
    }

    public CompositeFuture endFuture() {
        return CompositeFuture.all((List) ImmutableList.builder()
                .addAll(transactionConnnectionMap.values())
                .addAll(connnectionFutureCollection).build());
    }

    public abstract List<Observable<Object[]>> getObservableList(String node);

    public static final class HbtMycatDataContextImpl extends AsyncMycatDataContextImpl {

        public HbtMycatDataContextImpl(MycatDataContext dataContext, CodeExecuterContext context) {
            super(dataContext, context, DrdsRunnerHelper.preParse("select 1", null));
        }

        @Override
        public List<Observable<Object[]>> getObservableList(String node) {
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = codeExecuterContext.getRelContext().get(node);
            MycatTransientSQLTableScan relNode = (MycatTransientSQLTableScan) mycatRelDatasourceSourceInfo.getRelNode();
            ImmutableMultimap<String, SqlString> multimap = ImmutableMultimap.of(relNode.getTargetName(), new SqlString(MycatSqlDialect.DEFAULT, relNode.getSql()));
            return getObservables(multimap, mycatRelDatasourceSourceInfo.getColumnInfo());
        }

        @Override
        public Observable<Object[]> getObservable(String node, Function1 function1, Comparator comparator, int offset, int fetch) {
            return null;
        }
    }


    public static final class SqlMycatDataContextImpl extends AsyncMycatDataContextImpl {

        private DrdsSqlWithParams drdsSqlWithParams;
        private ConcurrentMap<String, List<PartitionGroup>> cache = new ConcurrentHashMap<>();


        public SqlMycatDataContextImpl(MycatDataContext dataContext, CodeExecuterContext context, DrdsSqlWithParams drdsSqlWithParams) {
            super(dataContext, context, drdsSqlWithParams);
            this.drdsSqlWithParams = drdsSqlWithParams;
        }

        public List<Observable<Object[]>> getObservableList(String node) {
            if (shareObservable.containsKey(node)) {
                return (shareObservable.get(node));
            }
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = this.codeExecuterContext.getRelContext().get(node);
            MycatView view = mycatRelDatasourceSourceInfo.getRelNode();
            List<PartitionGroup> sqlMap = getPartition(node).get();
            boolean share = mycatRelDatasourceSourceInfo.refCount > 0;
            List<Observable<Object[]>> observables = getObservables((view
                    .apply(mycatRelDatasourceSourceInfo.getSqlTemplate(), sqlMap, drdsSqlWithParams.getParams())), mycatRelDatasourceSourceInfo.getColumnInfo());
            if (share) {
                observables = observables.stream().map(i -> i.share()).collect(Collectors.toList());
                shareObservable.put(node, observables);
            }
            return observables;
        }

        public Optional<List<PartitionGroup>> getPartition(String node) {
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = this.codeExecuterContext.getRelContext().get(node);
            if (mycatRelDatasourceSourceInfo == null) return Optional.empty();
            MycatView view = mycatRelDatasourceSourceInfo.getRelNode();
            return Optional.ofNullable(cache.computeIfAbsent(node, s -> getSqlMap(codeExecuterContext.getConstantMap(), view, drdsSqlWithParams, drdsSqlWithParams.getHintDataNodeFilter())));
        }


        @Override
        public Observable<Object[]> getObservable(String node, Function1 function1, Comparator comparator, int offset, int fetch) {
            List<Observable<Object[]>> observableList = getObservableList(node);
            Iterable<Flowable<Object[]>> collect = observableList.stream().map(s -> Flowable.fromObservable(s, BackpressureStrategy.BUFFER)).collect(Collectors.toList());
            Flowable<Object[]> flowable = Flowables.orderedMerge(collect, (o1, o2) -> {
                Object left = function1.apply(o1);
                Object right = function1.apply(o2);
                return comparator.compare(left, right);
            });
            if (offset > 0) {
                flowable = flowable.skip(offset);
            }
            if (fetch > 0 && fetch != Integer.MAX_VALUE) {
                flowable = flowable.take(fetch);
            }
            return flowable.toObservable();
        }

    }


    @Override
    public Observable<Object[]> getObservable(String node) {
        return Observable.merge(getObservableList(node));
    }

    public static List<PartitionGroup> getSqlMap(Map<RexNode, RexNode> constantMap,
                                                 MycatView view,
                                                 DrdsSqlWithParams drdsSqlWithParams,
                                                 Optional<List<PartitionGroup>> hintDataMapping) {
        Distribution distribution = view.getDistribution();

        Distribution.Type type = distribution.type();
        switch (type) {
            case BROADCAST: {
                Map<String, Partition> builder = new HashMap<>();
                String targetName = null;
                for (GlobalTable globalTable : distribution.getGlobalTables()) {
                    if (targetName == null) {
                        int i = ThreadLocalRandom.current().nextInt(0, globalTable.getGlobalDataNode().size());
                        Partition partition = globalTable.getGlobalDataNode().get(i);
                        targetName = partition.getTargetName();
                    }
                    builder.put(globalTable.getUniqueName(), globalTable.getDataNode());
                }
                return Collections.singletonList(new PartitionGroup(targetName, builder));
            }
            case PHY:
                Map<String, Partition> builder = new HashMap<>();
                String targetName = null;
                for (GlobalTable globalTable : distribution.getGlobalTables()) {
                    builder.put(globalTable.getUniqueName(), globalTable.getDataNode());
                }
                for (NormalTable normalTable : distribution.getNormalTables()) {
                    if (targetName == null) {
                        targetName = normalTable.getDataNode().getTargetName();
                    }
                    builder.put(normalTable.getUniqueName(), normalTable.getDataNode());
                }
                return Collections.singletonList(new PartitionGroup(targetName, builder));
            case SHARDING:
                if (hintDataMapping.isPresent()) {
                    return hintDataMapping.get();
                }

                ShardingTable shardingTable = distribution.getShardingTables().get(0);
                RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
                RexNode condition = view.getCondition().orElse(MycatCalciteSupport.RexBuilder.makeLiteral(true));
                List<RexNode> inputConditions = new ArrayList<>(constantMap.size() + 1);

                inputConditions.add(condition);
                for (Map.Entry<RexNode, RexNode> rexNodeRexNodeEntry : constantMap.entrySet()) {
                    inputConditions.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexNodeRexNodeEntry.getKey(), rexNodeRexNodeEntry.getValue()));
                }
                ParamHolder paramHolder = ParamHolder.CURRENT_THREAD_LOCAL.get();
                paramHolder.setData(drdsSqlWithParams.getParams(), drdsSqlWithParams.getTypeNames());
                try {
                    ArrayList<RexNode> res = new ArrayList<>(inputConditions.size());
                    MycatRexExecutor.INSTANCE.reduce(rexBuilder, inputConditions, res);
                    condition = res.get(0);
                    ValuePredicateAnalyzer predicateAnalyzer = new ValuePredicateAnalyzer(shardingTable.keyMetas(), shardingTable.getColumns().stream().map(i -> i.getColumnName()).collect(Collectors.toList()));
                    ValueIndexCondition indexCondition = predicateAnalyzer.translateMatch(condition);
                    List<Partition> partitions = ValueIndexCondition.getObject(shardingTable.getShardingFuntion(), indexCondition, drdsSqlWithParams.getParams());
                    return mapSharding(view, partitions);
                } finally {
                    paramHolder.clear();
                }
            default:
                throw new IllegalStateException("Unexpected value: " + distribution.type());
        }
    }

    public static List<PartitionGroup> mapSharding(MycatView view, List<Partition> object) {
        Distribution distribution = view.getDistribution();
        ImmutableMap.Builder<String, Partition> globalbuilder = ImmutableMap.builder();
        for (GlobalTable globalTable : distribution.getGlobalTables()) {
            globalbuilder.put(globalTable.getUniqueName(), globalTable.getDataNode());
        }
        ImmutableMap<String, Partition> globalMap = globalbuilder.build();
        ShardingTable shardingTable = distribution.getShardingTables().get(0);
        String primaryTableUniqueName = shardingTable.getLogicTable().getUniqueName();
        List<Partition> primaryTableFilterPartitions = object;
//                Map<String, List<DataNode>> collect = this.shardingTables.stream()
//                        .collect(Collectors.toMap(k -> k.getUniqueName(), v -> v.getShardingFuntion().calculate(Collections.emptyMap())));
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        List<ShardingTable> shardingTables = metadataManager.getErTableGroup().getOrDefault(shardingTable.getShardingFuntion().getErUniqueID(), Collections.emptyList());
        Map<String, List<Partition>> collect = shardingTables.stream().collect(Collectors.toMap(k -> k.getUniqueName(), v -> v.dataNodes()));
        Map<Integer, String> mappingIndex = new HashMap<>();
        List<String> allDataNodeUniqueNames = collect.get(primaryTableUniqueName).stream().sequential().map(i -> i.getUniqueName()).collect(Collectors.toList());
        {

            for (Partition filterPartition : primaryTableFilterPartitions) {
                int index = 0;
                for (String allDataNodeUniqueName : allDataNodeUniqueNames) {
                    if (allDataNodeUniqueName.equals(filterPartition.getUniqueName())) {
                        mappingIndex.put(index, filterPartition.getTargetName());
                        break;
                    }
                    index++;
                }

            }
        }
        List<PartitionGroup> res = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : mappingIndex.entrySet()) {
            Integer index = entry.getKey();
            HashMap<String, Partition> map = new HashMap<>();
            for (Map.Entry<String, List<Partition>> stringListEntry : collect.entrySet()) {
                List<Partition> partitions = stringListEntry.getValue();
                if (partitions.size() > index) {
                    map.put(stringListEntry.getKey(), partitions.get(index));
                }else {
                    break;
                }
            }
            map.putAll(globalMap);
            res.add(new PartitionGroup(entry.getValue(), map));
        }
        return res;
    }
}
