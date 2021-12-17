//package io.ordinate.engine.factory;
//
//import com.google.common.collect.ImmutableList;
//import io.mycat.calcite.MycatRel;
//import io.mycat.calcite.logical.MycatView;
//import io.mycat.calcite.physical.*;
//import io.mycat.calcite.table.MycatTableScan;
//import io.ordinate.engine.builder.*;
//import io.ordinate.engine.function.Function;
//import io.ordinate.engine.function.IntFunction;
//import io.ordinate.engine.function.aggregate.AccumulatorFunction;
//import io.ordinate.engine.function.bind.VariableParameterFunction;
//import io.ordinate.engine.function.constant.IntConstant;
//import io.ordinate.engine.physicalplan.CorrelateJoinPlan;
//import io.ordinate.engine.physicalplan.PhysicalPlan;
//import io.ordinate.engine.physicalplan.ValuesPlan;
//import io.ordinate.engine.schema.InnerType;
//import io.questdb.std.Misc;
//import io.questdb.std.NumericException;
//import io.questdb.std.datetime.microtime.TimestampFormatUtils;
//import io.questdb.std.datetime.microtime.Timestamps;
//import io.questdb.std.str.CharSink;
//import org.apache.arrow.vector.types.pojo.Schema;
//import org.apache.calcite.linq4j.JoinType;
//import org.apache.calcite.rel.RelCollation;
//import org.apache.calcite.rel.RelFieldCollation;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.*;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeField;
//import org.apache.calcite.rex.RexLiteral;
//import org.apache.calcite.rex.RexNode;
//import org.apache.calcite.sql.SqlKind;
//import org.apache.calcite.sql.type.SqlTypeName;
//import org.apache.calcite.util.ImmutableBitSet;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static io.ordinate.engine.builder.ExecuteCompiler.unionAll;
//import static io.questdb.std.datetime.microtime.Timestamps.isLeapYear;
//
//public class MycatCalciteCompiler {
//    public static MycatCalciteCompiler create(MycatCalciteCompilerOptions options) {
//        return null;
//    }
//
//    public Factory compile(MycatRel mycatRel) {
//        if (mycatRel instanceof MycatView) {
//            return ViewFactory.of((MycatView) mycatRel);
//        }
//        if (mycatRel instanceof MycatProject) {
//            MycatProject mycatProject = (MycatProject) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatProject.getInput());
//            return ProjectFactory.of(mycatProject,inputFactory);
//        }
//        if (mycatRel instanceof MycatFilter) {
//            MycatFilter mycatFilter = (MycatFilter) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatFilter.getInput());
//            return FilterFactory.of(mycatFilter,inputFactory);
//        }
//        if (mycatRel instanceof MycatCalc) {
//            MycatCalc mycatCalc = (MycatCalc) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatCalc.getInput());
//            return CaclFactory.of((MycatCalc) mycatRel,inputFactory);
//        }
//        if (mycatRel instanceof MycatHashAggregate) {
//            MycatHashAggregate mycatHashAggregate = (MycatHashAggregate) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatHashAggregate.getInput());
//            return HashAggFactory.of((MycatHashAggregate) mycatRel,inputFactory);
//        }
//        if (mycatRel instanceof MycatSortAgg) {
//            MycatSortAgg mycatSortAgg = (MycatSortAgg) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatSortAgg.getInput());
//            return SortAggFactory.of((MycatSortAgg) mycatRel,inputFactory);
//        }
//        if (mycatRel instanceof MycatMemSort) {
//            MycatMemSort mycatMemSort = (MycatMemSort) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatMemSort.getInput());
//            return MenSortFactory.of((MycatMemSort) mycatRel,inputFactory);
//        }
//        if (mycatRel instanceof MycatTopN) {
//            MycatTopN mycatTopN = (MycatTopN) mycatRel;
//            Factory inputFactory = compile((MycatRel) mycatTopN.getInput());
//            return TopNSortFactory.of(mycatTopN,inputFactory);
//        }
//        if (mycatRel instanceof MycatTableScan){
//            MycatTableScan tableScan =(MycatTableScan)mycatRel;
//            return new VisualTablescanFactory(tableScan);
//        }
//        return null;
//    }
//    RexConverter rexConverter = new RexConverter();
//    Map<String, List<Object[]>> tableSource = new HashMap<>();
//
//    public void registerTable(String name, List<Object[]> physicalPlan){
//        tableSource.put(name,physicalPlan);
//    }
//    public static void main(String[] args) throws NumericException {
////        long l1 = TimestampFormatUtils.parseUTCTimestamp(Timestamp.valueOf(LocalDateTime.now()).toLocalDateTime().toString());
//        long l = Timestamps.toMicros(2021, isLeapYear(2021), 9, 18, 16, 1);
//        l = l + 9999991;
//        CharSink sink = Misc.getThreadLocalBuilder();
//        TimestampFormatUtils.USEC_UTC_FORMAT.format(l, null, "", sink);
//        String s = sink.toString();
//        System.out.println(s);
////        long l = Long.MAX_VALUE / (31556736 * 1000000L);
////        System.out.println(l);
////        CalciteCompiler calciteCompiler = new CalciteCompiler();
////        RexConverter rexConverter = new RexConverter();
////        ExecuteCompiler executeCompiler = new ExecuteCompiler();
////        RelNode relNode = LogicalValues.create();
//
//    }
//
//    public PhysicalPlan convert(RelNode relNode) {
//        if (relNode instanceof Values) {
//            Values values = (Values) relNode;
//            return convertValues(values);
//        } else if (relNode instanceof Filter) {
//            Filter filter = (Filter) relNode;
//            return convertFilter(filter);
//        } else if (relNode instanceof Project) {
//            Project project = (Project) relNode;
//            return convertProject(project);
//        } else if (relNode instanceof Join) {
//            Join join = (Join) relNode;
//            return convertJoin(join);
//        } else if (relNode instanceof Aggregate) {
//            Aggregate aggregate = (Aggregate) relNode;
//            return convertAggregate(aggregate);
//        } else if (relNode instanceof Union) {
//            Union union = (Union) relNode;
//            return convertUnion(union);
//        } else if (relNode instanceof Correlate) {
//            Correlate correlate = (Correlate) relNode;
//            return convertCorrelate(correlate);
//        } else if (relNode instanceof Sort) {
//            Sort sort = (Sort) relNode;
//            return convert(sort.getInput());
//            if (sort.isEnforcer()) {
//               return convertEnforce(sort);
//            } else if (sort.collation.getFieldCollations().isEmpty()){
//                return  convertLimit(sort.offset, sort.fetch);
//            }
//            convertEnforce(sort);
//            convertLimit(sort.offset, sort.fetch);
//            return this;
//        }else if (relNode instanceof TableScan){
//            List<String> qualifiedName = relNode.getTable().getQualifiedName();
//            String name = String.join(".", qualifiedName);
//            List<Object[]> physicalPlan = tableSource.get(name);
//            RelDataType rowType = relNode.getRowType();
//            List<InnerType> innerTypes = RexConverter.convertColumnTypeList(rowType);
//            Schema schema = SchemaBuilder.ofInnerTypes(innerTypes);
//            executeCompiler.stack.push(ValuesPlan.create(schema,physicalPlan));
//            return this;
//        }
//        throw new UnsupportedOperationException();
//    }
//
//    private PhysicalPlan convertLimit(PhysicalPlan input,RexNode offset, RexNode fetch) {
//        IntFunction offsetFunction = Optional.ofNullable(offset).map(i -> {
//            return (IntFunction) rexConverter.convertRex(offset,input.schema());
//        }).orElse(IntConstant.newInstance(0));
//        IntFunction fetchFunction =  Optional.ofNullable(fetch).map((i)->(IntFunction) rexConverter.convertRex(fetch,input.schema())).orElse(IntConstant.newInstance(Integer.MAX_VALUE));
//        ExecuteCompiler.li.limit(offsetFunction,fetchFunction);
//        return this;
//    }
//
//    private PhysicalPlan convertEnforce(Sort sort) {
//        List<PhysicalSortProperty> physicalSortProperties = getPhysicalSortProperties(sort);
//
//        executeCompiler.sort(physicalSortProperties);
//        return this;
//    }
//
//    public static List<PhysicalSortProperty> getPhysicalSortProperties(Sort sort) {
//        RelCollation collation = sort.collation;
//        List<PhysicalSortProperty> physicalSortProperties = new ArrayList<>();
//        List<RelDataTypeField> fieldList = sort.getRowType().getFieldList();
//        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
//            int fieldIndex = fieldCollation.getFieldIndex();
//            RelFieldCollation.Direction direction = fieldCollation.direction;
//            SortOptions sortOptions = new SortOptions();
//            switch (direction) {
//                case ASCENDING:
//                    sortOptions.descending = false;
//                    break;
//                case DESCENDING:
//                    sortOptions.descending = true;
//                    break;
//                case STRICTLY_ASCENDING:
//                case STRICTLY_DESCENDING:
//                case CLUSTERED:
//                    throw new UnsupportedOperationException();
//            }
//            switch (fieldCollation.nullDirection) {
//                case FIRST:
//                    sortOptions.nullsFirst = true;
//                    break;
//                case LAST:
//                case UNSPECIFIED:
//                    sortOptions.nullsFirst = false;
//                    break;
//            }
//            SqlTypeName sqlTypeName = fieldList.get(fieldIndex).getType().getSqlTypeName();
//            InnerType innerType = RexConverter.convertColumnType(sqlTypeName);
//            physicalSortProperties.add(PhysicalSortProperty.of(fieldIndex, sortOptions,innerType));
//        }
//        return physicalSortProperties;
//    }
//
//    private PhysicalPlan convertCorrelate(Correlate correlate) {
//        PhysicalPlan left = convert(correlate.getLeft());
//        PhysicalPlan right = convert(correlate.getRight());
//
//        CorrelationId correlationId = correlate.getCorrelationId();
//        List<Integer> requireList = correlate.getRequiredColumns().asList();
//        Map<CorrelationKey, List<VariableParameterFunction>> map = rexConverter.getVariableParameterFunctionMap();
//        List<Map.Entry<CorrelationKey, List<VariableParameterFunction>>> entryList = map.entrySet().stream().filter(i -> i.getKey().correlationId.getId() == correlationId.getId()).collect(Collectors.toList());
//
//
//        HashMap<Integer, List<VariableParameterFunction>> targetMap = new HashMap<>();
//        for (Map.Entry<CorrelationKey, List<VariableParameterFunction>> e : entryList) {
//            int index = e.getKey().index;
//            Integer integer = requireList.get(index);
//            for (VariableParameterFunction variableParameterFunction : e.getValue()) {
//                targetMap.compute(integer, (integer1, variableParameterFunctions) -> new ArrayList<>())
//                        .add(variableParameterFunction);
//            }
//
//        }
//        return new CorrelateJoinPlan(left,right,JoinType.valueOf(correlate.getJoinType().name()),targetMap);
//    }
//
//    private PhysicalPlan convertUnion(Union union) {
//        List<PhysicalPlan> inputs = new ArrayList<>();
//        for (RelNode input : union.getInputs()) {
//            inputs.add(convert(input));
//        }
//        return unionAll(union.all,inputs);
//    }
//
//    private PhysicalPlan convertAggregate(Aggregate aggregate) {
//        convert(aggregate.getInput());
//        ImmutableList<ImmutableBitSet> groupSets = aggregate.getGroupSets();
//        List<AggregateCall> aggCallList = aggregate.getAggCallList();
//        GroupKeys[] groupKeys = groupSets.stream().map(i -> GroupKeys.of(i.toArray())).toArray(n -> new GroupKeys[n]);
//        AccumulatorFunction[] accumulatorFunctions = new AccumulatorFunction[aggCallList.size()];
//        int index = 0;
//        for (AggregateCall aggregateCall : aggCallList) {
//            List<Integer> argList = aggregateCall.getArgList();
//            SqlKind kind = aggregateCall.getAggregation().kind;
//            AccumulatorFunction accumulatorFunction = null;
//            switch (kind) {
//                case SUM:
//                case SUM0: {
//                    accumulatorFunction = executeCompiler.sum(argList.get(0));
//                    break;
//                }
//                case AVG: {
//                    accumulatorFunction = executeCompiler.avg(argList.get(0));
//                    break;
//                }
//                case COUNT: {
//                    boolean distinct = aggregateCall.isDistinct();
//                    if (distinct) {
//                        //todo check
//                        accumulatorFunction = executeCompiler.countDistinct(argList.get(0));
//                    } else {
//                        if (argList.size() == 0) {
//                            accumulatorFunction = executeCompiler.count();
//                        } else {
//                            accumulatorFunction = executeCompiler.count(argList.get(0));
//                        }
//                    }
//                    break;
//                }
//                case ANY_VALUE: {
//                    accumulatorFunction = executeCompiler.anyValue(argList.get(0));
//                    break;
//                }
//                case MAX: {
//                    accumulatorFunction = executeCompiler.max(argList.get(0));
//                    break;
//                }
//                case MIN: {
//                    accumulatorFunction = executeCompiler.min(argList.get(0));
//                    break;
//                }
//            }
//            Objects.requireNonNull(accumulatorFunction);
//            accumulatorFunctions[index] = accumulatorFunction;
//            ++index;
//        }
//        executeCompiler.agg(ExecuteCompiler.AggImpl.HASH, Arrays.asList(groupKeys), Arrays.asList(accumulatorFunctions));
//
//        return this;
//    }
//
//    private PhysicalPlan convertJoin(Join join) {
//        JoinRelType joinType = join.getJoinType();
//        PhysicalPlan left = convert(join.getLeft());
//        PhysicalPlan right = convert(join.getRight());
//        return  executeCompiler.crossJoin(JoinType.valueOf(joinType.name()), ExecuteCompiler.JoinImpl.HASH, rexConverter.convertRex(join.getCondition(),));
//    }
//
//    private PhysicalPlan convertProject(Project project) {
//        PhysicalPlan input =convert(project.getInput());
//        List<RexNode> projects = project.getProjects();
//
//        int index = 0;
//        Function[] functions = new Function[projects.size()];
//        for (RexNode rexNode : projects) {
//            Function function = rexConverter.convertRex(rexNode,input.schema());
//            functions[index] = function;
//            index++;
//        }
//        return executeCompiler.project(input,functions);
//    }
//
//    private PhysicalPlan convertFilter(Filter filter) {
//        PhysicalPlan input = convert(filter.getInput());
//        RexNode condition = filter.getCondition();
//        Function function = rexConverter.convertRex(condition,input.schema());
//      return   executeCompiler.filter(input,function);
//    }
//
//    public PhysicalPlan convertValues(Values values) {
//        ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();
//
//        ArrayList<Function[]> rowList = new ArrayList<>();
//        for (ImmutableList<RexLiteral> tuple : tuples) {
//            int size = tuple.size();
//            Function[] functions = new Function[size];
//            int index = 0;
//            for (RexLiteral rexLiteral : tuple) {
//                Function function = rexConverter.convertToFunction(rexLiteral);
//                functions[index] = function;
//                index++;
//            }
//            rowList.add(functions);
//        }
//       return executeCompiler.values(rowList);
//        return this;
//    }
//
//    public PhysicalPlan build() {
//        return executeCompiler.build();
//    }
//}
