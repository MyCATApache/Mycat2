package io.ordinate.engine.physicalplan;

import com.google.common.collect.ImmutableMultimap;
import io.mycat.AsyncMycatDataContextImpl;
import io.mycat.DrdsSql;
import io.mycat.DrdsSqlWithParams;
import io.mycat.PartitionGroup;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.calcite.MycatRelDatasourceSourceInfo;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.vertx.VertxExecuter;
import io.ordinate.engine.builder.CalciteCompiler;
import io.ordinate.engine.builder.PhysicalSortProperty;
import io.ordinate.engine.factory.FactoryUtil;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.record.RootContext;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MycatViewPlan implements PhysicalPlan {
    final MycatView mycatView;
    final Schema schema;
    private IntFunction offset;
    private IntFunction fecth;

    public MycatViewPlan(MycatView mycatView, IntFunction offset, IntFunction fecth) {
        this.mycatView = mycatView;
        this.schema = FactoryUtil.toArrowSchema(mycatView.getMycatRelDataTypeByCalcite());
        this.offset = offset;
        this.fecth = fecth;
    }

    @Override
    public Schema schema() {
        return this.schema;
    }

    @Override
    public List<PhysicalPlan> children() {
        return Collections.emptyList();
    }

    @Override
    public Observable<VectorSchemaRoot> execute(RootContext rootContext) {
        RootAllocator rootAllocator = new RootAllocator();
        NewMycatDataContext context = (NewMycatDataContext) rootContext.getContext();
        DrdsSqlWithParams drdsSql = context.getDrdsSql();
        MycatView view = (MycatView) mycatView;
        CopyMycatRowMetaData rowMetaData = new CopyMycatRowMetaData(
                new CalciteRowMetaData(view.getRowType().getFieldList()));
        MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo = new MycatRelDatasourceSourceInfo(
                rowMetaData,
                view.getSQLTemplate(DrdsSql.isForUpdate(drdsSql.getParameterizedStatement())),
                view);
        SqlNode sqlTemplate = mycatRelDatasourceSourceInfo.getSqlTemplate();
        List<PartitionGroup> partitionGroups = AsyncMycatDataContextImpl.getSqlMap(Collections.emptyMap(), view, drdsSql, drdsSql.getHintDataNodeFilter());
        ImmutableMultimap<String, SqlString> stringSqlStringImmutableMultimap = view.apply(-1, sqlTemplate, partitionGroups, drdsSql.getParams());

        List<Observable<VectorSchemaRoot>> observableList = new ArrayList<>();
        for (Map.Entry<String, SqlString> entry : stringSqlStringImmutableMultimap.entries()) {
            String key = entry.getKey();
            SqlString sqlString = entry.getValue();

            observableList.add(Observable.create(emitter -> {
                Future<NewMycatConnection> connectionFuture = context.getConnection(
                        context.getContext().resolveDatasourceTargetName(key));
                Future<Observable<VectorSchemaRoot>> observableFuture = connectionFuture.map(connection -> {
                    Observable<VectorSchemaRoot> observable = connection.prepareQuery(sqlString.getSql(),
                            MycatPreparedStatementUtil.extractParams(drdsSql.getParams(), sqlString.getDynamicParameters()),
                            rootAllocator);
                    return observable.doOnComplete(() -> context.recycleConnection(   context.getContext().resolveDatasourceTargetName(key), Future.succeededFuture(connection)));
                });
                observableFuture.onFailure(event -> emitter.tryOnError(event));
                observableFuture.onSuccess(event -> {

                    event=  event.doOnComplete(() -> emitter.onComplete());
                    event=   event.doOnError(throwable -> emitter.tryOnError(throwable));
                  event.forEach(vectorSchemaRoot -> emitter.onNext(vectorSchemaRoot));
                });
            }));
        }
        return Observable.fromIterable(observableList).flatMap(i->i);



    }
//    Schema schema = FactoryUtil.toArrowSchema(view.getMycatRelDataTypeByCalcite());
//        if (view.getDistribution().type() == Distribution.Type.SHARDING) {
//        if (view.getRelNode() instanceof Sort) {
//            Sort viewRelNode = (Sort) view.getRelNode();
//
//            Integer offset;
//            Integer fetch;
//            if (viewRelNode.offset != null || viewRelNode.fetch != null) {
//                offset = this.offset.getInt(null);
//                fetch = this.fecth.getInt(null);
//                List<Observable<Object[]>> observableList = context.getObservableList(view.getDigest());
//                RelCollation collation = view.getTraitSet().getCollation();
//                List<PhysicalSortProperty> physicalSortProperties = CalciteCompiler.getPhysicalSortProperties(viewRelNode);
//                return new MergeSortObjectArray(schema, observableList, physicalSortProperties, offset, fetch).execute(rootContext);
//
//            }
//            {
//                List<Observable<Object[]>> observableList = context.getObservableList(view.getDigest());
//                List<PhysicalSortProperty> physicalSortProperties = CalciteCompiler.getPhysicalSortProperties(viewRelNode);
//                return new MergeSortObjectArray(schema, observableList, physicalSortProperties, 0, Integer.MAX_VALUE).execute(rootContext);
//            }
//        }
//    }
//        return new UnionAllObjectArray(schema, context.getObservableList(view.getDigest())).execute(rootContext);
    @Override
    public void accept(PhysicalPlanVisitor physicalPlanVisitor) {

    }
}
