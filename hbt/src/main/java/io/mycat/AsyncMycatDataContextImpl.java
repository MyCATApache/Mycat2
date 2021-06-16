package io.mycat;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import hu.akarnokd.rxjava3.operators.Flowables;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.*;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.calcite.rewriter.PredicateAnalyzer;
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
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

public abstract class AsyncMycatDataContextImpl extends NewMycatDataContextImpl {
    final Map<String, Future<SqlConnection>> usedConnnectionMap = new HashMap<>();
    final Map<String, List<Observable<Object[]>>> shareObservable = new HashMap<>();

    public AsyncMycatDataContextImpl(MycatDataContext dataContext,
                                     CodeExecuterContext context,
                                     List<Object> params) {
        super(dataContext, context, params);
    }

    @NotNull
    public List<Observable<Object[]>> getObservables(ImmutableMultimap<String, SqlString> expand, MycatRowMetaData calciteRowMetaData) {
        LinkedList<Observable<Object[]>> observables = new LinkedList<>();
        XaSqlConnection transactionSession = (XaSqlConnection) context.getTransactionSession();
        for (Map.Entry<String, SqlString> entry : expand.entries()) {
            String key = context.resolveDatasourceTargetName(entry.getKey());
            SqlString sqlString = entry.getValue();
            Observable<Object[]> observable = Observable.create(emitter -> {
                synchronized (usedConnnectionMap) {
                    Future<SqlConnection> sessionConnection = usedConnnectionMap
                            .computeIfAbsent(key, s -> transactionSession.getConnection(key));
                    PromiseInternal<SqlConnection> promise = VertxUtil.newPromise();
                    Observable<Object[]> innerObservable = Objects.requireNonNull(VertxExecuter.runQuery(sessionConnection,
                            sqlString.getSql(),
                            MycatPreparedStatementUtil.extractParams(params, sqlString.getDynamicParameters()), calciteRowMetaData));
                    innerObservable.subscribe(objects -> {
                                emitter.onNext((objects));
                            },
                            throwable -> {
                                sessionConnection.onSuccess(c -> {
                                    promise.fail(throwable);
                                })
                                        .onFailure(t -> promise.fail(t));
                            }, () -> {
                                sessionConnection.onSuccess(c -> {
                                    promise.tryComplete(c);
                                }).onFailure(t -> promise.fail(t));
                                ;
                            });
                    usedConnnectionMap.put(key,
                            promise.future()
                                    .onSuccess(c -> {
                                        emitter.onComplete();
                                    })
                                    .onFailure(t -> {
                                        emitter.onError(t);
                                    }));
                }
            });
            observables.add(observable);
        }
        return observables;
    }

    public CompositeFuture endFuture() {
        return CompositeFuture.all(new ArrayList<>(usedConnnectionMap.values()));
    }

    public abstract List<Observable<Object[]>> getObservableList(String node);

    public static final class HbtMycatDataContextImpl extends AsyncMycatDataContextImpl {

        public HbtMycatDataContextImpl(MycatDataContext dataContext, CodeExecuterContext context) {
            super(dataContext, context, Collections.emptyList());
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


        public SqlMycatDataContextImpl(MycatDataContext dataContext, CodeExecuterContext context, DrdsSqlWithParams drdsSqlWithParams) {
            super(dataContext, context, drdsSqlWithParams.getParams());
            this.drdsSqlWithParams = drdsSqlWithParams;
        }

        public List<Observable<Object[]>> getObservableList(String node) {
            if (shareObservable.containsKey(node)) {
                return (shareObservable.get(node));
            }
            MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = this.codeExecuterContext.getRelContext().get(node);
            MycatView view = mycatRelDatasourceSourceInfo.getRelNode();
            List<Map<String, Partition>> sqlMap = getSqlMap(view, drdsSqlWithParams,drdsSqlWithParams.getHintDataNodeFilter());
            boolean share = mycatRelDatasourceSourceInfo.refCount > 0;
            List<Observable<Object[]>> observables = getObservables((view
                    .apply(mycatRelDatasourceSourceInfo.getSqlTemplate(), sqlMap, params)), mycatRelDatasourceSourceInfo.getColumnInfo());
            if (share) {
                observables = observables.stream().map(i -> i.share()).collect(Collectors.toList());
                shareObservable.put(node, observables);
            }
            return observables;
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

    public static List<Map<String, Partition>> getSqlMap(MycatView view,
                                                         DrdsSqlWithParams drdsSqlWithParams,
                                                         Optional<List<Map<String, Partition>>> hintDataMapping) {
        Distribution distribution = view.getDistribution();

        Distribution.Type type = distribution.type();
        switch (type) {
            case BROADCAST:
            case PHY:
                Map<String, Partition> builder = new HashMap<>();
                for (NormalTable normalTable : distribution.getNormalTables()) {
                    builder.put(normalTable.getUniqueName(), normalTable.getDataNode());
                }
                for (GlobalTable globalTable : distribution.getGlobalTables()) {
                    builder.put(globalTable.getUniqueName(), globalTable.getDataNode());
                }
                return Collections.singletonList(builder);
            case SHARDING:
                if (hintDataMapping.isPresent()) {
                    return hintDataMapping.get();
                }
                ShardingTable shardingTable = distribution.getShardingTables().get(0);
                RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
                RexNode condition = view.getCondition().orElse(MycatCalciteSupport.RexBuilder.makeLiteral(true));
                ArrayList<RexNode> res = new ArrayList<>();
                MycatRexExecutor.INSTANCE.reduce(rexBuilder, Collections.singletonList(condition), res);
                if (!res.isEmpty()) {
                    condition = res.get(0);
                }
                PredicateAnalyzer predicateAnalyzer = new PredicateAnalyzer(shardingTable.keyMetas(), shardingTable.getColumns().stream().map(i -> i.getColumnName()).collect(Collectors.toList()));
                IndexCondition indexCondition = predicateAnalyzer.translateMatch(condition);
                List<Partition> partitions = IndexCondition.getObject(shardingTable.getShardingFuntion(), indexCondition, drdsSqlWithParams.getParams());

                return mapSharding(view, partitions);
            default:
                throw new IllegalStateException("Unexpected value: " + distribution.type());
        }
    }

    public static List<Map<String, Partition>> mapSharding(MycatView view, List<Partition> object) {
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
        List<Integer> mappingIndex = new ArrayList<>();
        List<String> allDataNodeUniqueNames = collect.get(primaryTableUniqueName).stream().sequential().map(i -> i.getUniqueName()).collect(Collectors.toList());
        {

            for (Partition filterPartition : primaryTableFilterPartitions) {
                int index = 0;
                for (String allDataNodeUniqueName : allDataNodeUniqueNames) {
                    if (allDataNodeUniqueName.equals(filterPartition.getUniqueName())) {
                        mappingIndex.add(index);
                        break;
                    }
                    index++;
                }

            }
        }
        TreeMap<Integer, Map<String, Partition>> res = new TreeMap<>();
        {
            for (Map.Entry<String, List<Partition>> e : collect.entrySet()) {
                String key = e.getKey();
                List<Partition> partitions = e.getValue();
                for (Integer integer : mappingIndex) {
                    Map<String, Partition> stringDataNodeMap = res.computeIfAbsent(integer, integer1 -> new HashMap<>());
                    stringDataNodeMap.put(key, partitions.get(integer));
                    stringDataNodeMap.putAll(globalMap);
                }
            }
        }
        return new ArrayList<>(res.values());
    }
}
