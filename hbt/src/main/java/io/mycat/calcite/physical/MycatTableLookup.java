//package io.mycat.calcite.physical;
//
//import io.mycat.AsyncMycatDataContextImpl;
//import io.mycat.beans.mycat.MycatRowMetaData;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.calcite.MycatConvention;
//import io.mycat.calcite.MycatRel;
//import io.mycat.calcite.resultset.CalciteRowMetaData;
//import io.reactivex.rxjava3.core.Observable;
//import io.reactivex.rxjava3.functions.Function;
//import org.apache.calcite.adapter.enumerable.*;
//import org.apache.calcite.linq4j.Enumerable;
//import org.apache.calcite.linq4j.function.EqualityComparer;
//import org.apache.calcite.linq4j.function.Function1;
//import org.apache.calcite.linq4j.function.Function2;
//import org.apache.calcite.linq4j.tree.*;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelTraitSet;
//import org.apache.calcite.rel.BiRel;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.RelWriter;
//import org.apache.calcite.rel.SingleRel;
//import org.apache.calcite.runtime.NewMycatDataContext;
//import org.apache.calcite.util.ImmutableBitSet;
//import org.apache.calcite.util.RxBuiltInMethodImpl;
//import org.omg.CORBA.Object;
//
//import java.lang.reflect.Method;
//import java.util.ArrayList;
//import java.util.List;
//
//public abstract class MycatTableLookup extends BiRel implements MycatRel {
//
//
//    public MycatTableLookup(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelNode right) {
//        super(cluster, traits.replace(MycatConvention.INSTANCE), input,right);
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        return null;
//    }
//
//    @Override
//    public void explain(RelWriter pw) {
//        super.explain(pw);
//    }
//
//    @Override
//    public RelWriter explainTerms(RelWriter pw) {
//        return super.explainTerms(pw);
//    }
//
//    @Override
//    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
////        Project project = (Project) this.right;
////        Filter filter = project.getInput() instanceof Filter ? (Filter) project.getInput() : null;
////        Join join = filter != null ? (Join) filter.getInput() : (Join) project.getInput();
////        Values left = (Values) join.getLeft();
////        TableScan right = (TableScan) join.getRight();
//        BlockBuilder builder = new BlockBuilder();
//        final Result leftResult =
//                implementor.visitChild(this, 0, (EnumerableRel) getInput(0), pref);
//        Expression leftExpression =
//                toEnumerate(builder.append(
//                        "left", leftResult.block));
//
//        ParameterExpression root = implementor.getRootExpression();
//        Method dispatch = Types.lookupMethod(MycatTableLookup.class, "dispatch", NewMycatDataContext.class, MycatTableLookup.class, Observable.class);
//        final PhysType physType =
//                PhysTypeImpl.of(
//                        implementor.getTypeFactory(),
//                        getRowType(),
//                        JavaRowFormat.ARRAY);
//        builder.add(Expressions.call(dispatch, root, implementor.stash(this, MycatTableLookup.class), toObservable(leftExpression)));
//        return implementor.result(physType, builder.toBlock());
//    }
//
//    public abstract Observable<Object[]> make(NewMycatDataContext context, List<Object[]> left);
//
//    public static java.lang.Object dispatch(NewMycatDataContext context, MycatTableLookup mycatTableLookup, Observable<Object[]> keys) {
//        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = (AsyncMycatDataContextImpl.SqlMycatDataContextImpl) context;
//        ImmutableBitSet joinKeys = mycatTableLookup.getJoinKeys();
//        boolean needJoin = !joinKeys.isEmpty();
//        MycatRowMetaData rightRowMetaData = new CalciteRowMetaData(mycatTableLookup.right.getRowType().getFieldList());
//        Function1 outerKeySelector = mycatTableLookup.getOuterKeySelector();
//        Function1 innerKeySelector = mycatTableLookup.getInnerKeySelector();
//        Function2 resultSelector = mycatTableLookup.getResultSelector();
//        EqualityComparer comparer = mycatTableLookup.getComparer();
//
//        Observable<Object[]> observable = keys;
//        if (needJoin) {
//            observable = observable.share();
//        } else {
//            observable = observable;
//        }
//        Observable<Object[]> right = observable.buffer(300, 50).flatMap((Function<List<Object[]>, Observable<Object[]>>) objects -> {
//            ArrayList<Object[]> left = new ArrayList<>();
//            for (Object[] object : objects) {
//                left.add((Object[]) outerKeySelector.apply(object));
//            }
//            return mycatTableLookup.make(context,left);
//        });
//
//        if (!needJoin) {
//            return right;
//        } else {
//            Enumerable<java.lang.Object[]> leftInput = RxBuiltInMethodImpl.toEnumerable(observable);
//            Enumerable<java.lang.Object[]> rightInput = RxBuiltInMethodImpl.toEnumerable(right);
//            return leftInput.hashJoin(rightInput, outerKeySelector, innerKeySelector, resultSelector, comparer);
//        }
//    }
//
//    public abstract ImmutableBitSet getJoinKeys();
//
//
//    public abstract EqualityComparer getComparer();
//
//    public abstract Function2 getResultSelector();
//
//    public abstract Function1 getInnerKeySelector();
//
//    public abstract Function1 getOuterKeySelector();
//
////
////    private static Map<String, List<SqlString>> route(ShardingTable shardingTable, MycatTableLookup relNode, List<Object[]> keys) {
////        SqlNode sqlNode = MycatCalciteSupport.INSTANCE.convertToSqlTemplate(relNode.right, MycatSqlDialect.DEFAULT, false);
////        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sqlNode.toString());
////        ImmutableMultimap<Partition, List<Object[]>> nodeListMap = routeKeys(shardingTable, relNode, keys);
////        Map<String, List<SqlString>> sqls = new HashMap<>();
////        for (Pair<Partition, List<Object[]>> dataNodeListPair : nodeListMap.keyValuePairsView()) {
////            Partition partition = dataNodeListPair.getOne();
////            List<Object[]> objects = dataNodeListPair.getTwo();
////            SQLStatement curStatement = sqlStatement.clone();
////            curStatement.accept(new MySqlASTVisitorAdapter() {
////                @Override
////                public boolean visit(SQLValuesTableSource x) {
////                    List<SQLName> columns = x.getColumns();
////                    List<SQLListExpr> values = x.getValues();
////                    values.clear();
////                    for (Object[] key : objects) {
////                        SQLListExpr sqlListExpr = new SQLListExpr();
////                        for (Object o : key) {
////                            sqlListExpr.addItem(SQLExprUtils.fromJavaObject(o));
////                        }
////                        values.add(sqlListExpr);
////                    }
////                    return false;
////                }
////
////                @Override
////                public boolean visit(SQLExprTableSource x) {
////                    x.setSimpleName(partition.getTable());
////                    x.setSchema(partition.getSchema());
////                    return false;
////                }
////            });
////            List<SqlString> strings = sqls.computeIfAbsent(partition.getTargetName(), s -> new ArrayList<>());
////            strings.add(new SqlString(MycatSqlDialect.DEFAULT, curStatement.toString(), ImmutableList.of()));
////        }
////        return sqls;
////    }
////
////    private static ImmutableMultimap<Partition, List<Object[]>> routeKeys(ShardingTable shardingTable, MycatTableLookup relNode, List<Object[]> keys) {
////        CustomRuleFunction shardingFuntion = shardingTable.getShardingFuntion();
////        List<String> fieldNames = relNode.getInput().getRowType().getFieldNames();
////        SimpleColumnInfo[] columns = new SimpleColumnInfo[fieldNames.size()];
////        int index = 0;
////        for (String fieldName : fieldNames) {
////            if (shardingFuntion.isShardingKey(fieldName)) {
////                columns[index] = shardingTable.getColumnByName(fieldName);
////            }
////            index++;
////        }
////        ImmutableListMultimap.Builder<Partition, List<Object[]>> resBuilder = ImmutableListMultimap.builder();
////
////        for (Object[] key : keys) {
////            ImmutableMap.Builder<String, Collection<RangeVariable>> builder = ImmutableMap.builder();
////            for (int i = 0; i < columns.length; i++) {
////                SimpleColumnInfo column = columns[i];
////                if (column != null) {
////                    String columnName = column.getColumnName();
////                    builder.put(columnName, Collections.singleton(new RangeVariable(columnName, RangeVariableType.EQUAL, column.normalizeValue(key[i]))));
////                }
////            }
////            List<Partition> partitions = shardingTable.getShardingFuntion().calculate(builder.build());
////            for (Partition partition : partitions) {
////                resBuilder.put(partition, keys);
////            }
////        }
////        return (ImmutableMultimap<Partition, List<Object[]>>) resBuilder.build();
////    }
//
////    public static Enumerable<Object[]> dispatch(NewMycatDataContext context, MycatTableLookup mycatTableLookup, List<Object[]> keys) {
////        List<RelOptTable> allTables = RelOptUtil.findAllTables(mycatTableLookup.right);
////        RelOptTable relOptTable = allTables.get(0);
////        MycatLogicTable mycatLogicTable = relOptTable.unwrap(MycatLogicTable.class);
////        ShardingTable shardingTable = (ShardingTable) mycatLogicTable.logicTable();
////        return context.getEnumerable(ExecutorSupport.physicalSqlMerge(route(shardingTable,mycatTableLookup, keys), false));
////    }
//
////    @Override
////    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
////        return new MycatTableLookup(getCluster(), traitSet, inputs.get(0), inputs.get(1));
////    }
//}
