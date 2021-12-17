/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.builder;

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.MycatRelDataTypeUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.table.MycatTableScan;
import io.ordinate.engine.factory.FactoryUtil;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.IntFunction;
import io.ordinate.engine.function.aggregate.AccumulatorFunction;
import io.ordinate.engine.function.aggregate.any.AnyAccumulator;
import io.ordinate.engine.function.bind.VariableParameterFunction;
import io.ordinate.engine.function.column.ColumnFunction;
import io.ordinate.engine.function.constant.IntConstant;
import io.ordinate.engine.physicalplan.*;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.vector.VectorExpression;
import lombok.Getter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;


import java.util.*;
import java.util.stream.Collectors;

@Getter
public class CalciteCompiler {
    static ExecuteCompiler executeCompiler = new ExecuteCompiler();
    final RexConverter rexConverter = new RexConverter();

    public PhysicalPlan convert(RelNode relNode) {
        if (relNode instanceof Values) {
            Values values = (Values) relNode;
            return convertValues(values);
        } else if (relNode instanceof Filter) {
            Filter filter = (Filter) relNode;
            return convertFilter(filter);
        } else if (relNode instanceof Project) {
            Project project = (Project) relNode;
            return convertProject(project);
        } else if (relNode instanceof Join) {
            Join join = (Join) relNode;
            return convertJoin(join);
        } else if (relNode instanceof Aggregate) {
            Aggregate aggregate = (Aggregate) relNode;
            return convertAggregate(aggregate);
        } else if (relNode instanceof Union) {
            Union union = (Union) relNode;
            return convertUnion(union);
        } else if (relNode instanceof Correlate) {
            Correlate correlate = (Correlate) relNode;
            return convertCorrelate(correlate);
        } else if (relNode instanceof Sort) {
            return convertTopN((Sort) relNode);
        } else if (relNode instanceof MycatTableScan) {
            return new VisualTablePlanImpl((MycatTableScan) relNode);
        } else if (relNode instanceof MycatView) {
            MycatView mycatView = (MycatView) relNode;
            RelNode viewRelNode = mycatView.getRelNode();
            IntFunction fetchFunction = null;
            IntFunction offsetFunction = null;
            if (viewRelNode instanceof Sort){
                Sort sort = (Sort) viewRelNode;
                RexNode fetch = sort.fetch;
                RexNode offset = sort.offset;



                if (offset!=null){
                    offsetFunction =(IntFunction) rexConverter.convertRex(offset, null);
                }
                if (fetch!=null){
                    if(fetch.isA(SqlKind.PLUS)){
                        offset = ((RexCall) fetch).getOperands().get(0);
                        offsetFunction =(IntFunction) rexConverter.convertRex(offset, null);

                        fetch = ((RexCall) fetch).getOperands().get(1);
                        fetchFunction =(IntFunction) rexConverter.convertRex(fetch, null);
                    }else {
                        fetchFunction =(IntFunction) rexConverter.convertRex(fetch, null);
                    }
                }
            }
            return new MycatViewPlan(mycatView,offsetFunction,fetchFunction);
        }
        throw new UnsupportedOperationException();
    }

    public PhysicalPlan convertTopN(Sort relNode) {
        PhysicalPlan physicalPlan = convert(relNode.getInput());
        return convertTopN(relNode, physicalPlan);
    }

    public PhysicalPlan convertTopN(Sort relNode, PhysicalPlan physicalPlan) {
        if (relNode.isEnforcer()) {
            return convertEnforce(physicalPlan, relNode);
        } else if (relNode.collation.getFieldCollations().isEmpty()) {
            return convertLimit(physicalPlan, relNode.offset, relNode.fetch);
        }
        physicalPlan = convertEnforce(physicalPlan, relNode);
        return convertLimit(physicalPlan, relNode.offset, relNode.fetch);
    }

    public PhysicalPlan convertLimit(PhysicalPlan input, RexNode offset, RexNode fetch) {
        IntFunction offsetFunction = Optional.ofNullable(offset).map(i -> {
            return (IntFunction) rexConverter.convertRex(offset, input.schema());
        }).orElse(IntConstant.newInstance(0));
        IntFunction fetchFunction = Optional.ofNullable(fetch).map((i) -> (IntFunction) rexConverter.convertRex(fetch, input.schema())).orElse(IntConstant.newInstance(Integer.MAX_VALUE));
        return LimitPlan.create(input, offsetFunction, fetchFunction);
    }

    public static PhysicalPlan convertEnforce(PhysicalPlan input, Sort sort) {
        List<PhysicalSortProperty> physicalSortProperties = getPhysicalSortProperties(sort);

        return SortPlan.create(input, physicalSortProperties);
    }

    public static List<PhysicalSortProperty> getPhysicalSortProperties(Sort sort) {
        RelCollation collation = sort.collation;
        List<PhysicalSortProperty> physicalSortProperties = new ArrayList<>();
        List<RelDataTypeField> fieldList = sort.getRowType().getFieldList();
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            int fieldIndex = fieldCollation.getFieldIndex();
            RelFieldCollation.Direction direction = fieldCollation.direction;
            SortOptions sortOptions = new SortOptions();
            switch (direction) {
                case ASCENDING:
                    sortOptions.descending = false;
                    break;
                case DESCENDING:
                    sortOptions.descending = true;
                    break;
                case STRICTLY_ASCENDING:
                case STRICTLY_DESCENDING:
                case CLUSTERED:
                    throw new UnsupportedOperationException();
            }
            switch (fieldCollation.nullDirection) {
                case FIRST:
                    sortOptions.nullsFirst = true;
                    break;
                case LAST:
                case UNSPECIFIED:
                    sortOptions.nullsFirst = false;
                    break;
            }
            SqlTypeName sqlTypeName = fieldList.get(fieldIndex).getType().getSqlTypeName();
            InnerType innerType = RexConverter.convertColumnType(sqlTypeName);
            physicalSortProperties.add(PhysicalSortProperty.of(fieldIndex, sortOptions, innerType));
        }
        return physicalSortProperties;
    }

    public CorrelateJoinPlan convertCorrelate(Correlate correlate) {
        PhysicalPlan left = convert(correlate.getLeft());
        PhysicalPlan right = convert(correlate.getRight());

        CorrelationId correlationId = correlate.getCorrelationId();
        List<Integer> requireList = correlate.getRequiredColumns().asList();
        Map<CorrelationKey, List<VariableParameterFunction>> map = rexConverter.getVariableParameterFunctionMap();
        List<Map.Entry<CorrelationKey, List<VariableParameterFunction>>> entryList = map.entrySet().stream().filter(i -> i.getKey().correlationId.getId() == correlationId.getId()).collect(Collectors.toList());


        HashMap<Integer, List<VariableParameterFunction>> targetMap = new HashMap<>();
        for (Map.Entry<CorrelationKey, List<VariableParameterFunction>> e : entryList) {
            int index = e.getKey().index;
            Integer integer = requireList.get(index);
            for (VariableParameterFunction variableParameterFunction : e.getValue()) {
                targetMap.compute(integer, (integer1, variableParameterFunctions) -> new ArrayList<>())
                        .add(variableParameterFunction);
            }

        }
        return new CorrelateJoinPlan(left, right, JoinType.valueOf(correlate.getJoinType().name()), targetMap);
    }

    public PhysicalPlan convertUnion(Union union) {
        List<PhysicalPlan> inputs = new ArrayList<>();
        for (RelNode input : union.getInputs()) {
            inputs.add(convert(input));
        }
        return executeCompiler.unionAll(union.all, inputs);
    }

    public PhysicalPlan convertAggregate(Aggregate aggregate) {

        PhysicalPlan input = convert(aggregate.getInput());
        List<Integer> groupSet = aggregate.getGroupSet().asList();
        List<AggregateCall> aggCallList = aggregate.getAggCallList();
        GroupKeys[] groupSets = aggregate.getGroupSets().stream().map(i -> GroupKeys.of(i.toArray())).toArray(n -> new GroupKeys[n]);
        AccumulatorFunction[] accumulatorFunctions = new AccumulatorFunction[groupSet.size() + aggCallList.size()];

        int index = 0;
        for (Integer integer : groupSet) {
            accumulatorFunctions[index] = ExecuteCompiler.anyValue(input,integer);
            index++;
        }


        index = groupSet.size();
        for (AggregateCall aggregateCall : aggCallList) {
            List<Integer> argList = aggregateCall.getArgList();
            SqlKind kind = aggregateCall.getAggregation().kind;
            AccumulatorFunction accumulatorFunction = null;
            switch (kind) {
                case SUM:
                case SUM0: {
                    Integer integer = argList.get(0);
                    accumulatorFunction = executeCompiler.sum(input, integer);
                    break;
                }
                case AVG: {
                    accumulatorFunction = executeCompiler.avg(argList.get(0));
                    break;
                }
                case COUNT: {
                    boolean distinct = aggregateCall.isDistinct();
                    if (distinct) {
                        //todo check
                        accumulatorFunction = executeCompiler.countDistinct(input, argList.get(0));
                    } else {
                        if (argList.size() == 0) {
                            accumulatorFunction = executeCompiler.count();
                        } else {
                            accumulatorFunction = executeCompiler.count(argList.get(0));
                        }
                    }
                    break;
                }
                case ANY_VALUE: {
                    accumulatorFunction = executeCompiler.anyValue(input, argList.get(0));
                    break;
                }
                case MAX: {
                    accumulatorFunction = executeCompiler.max(input, argList.get(0));
                    break;
                }
                case MIN: {
                    accumulatorFunction = executeCompiler.min(input, argList.get(0));
                    break;
                }
            }
            Objects.requireNonNull(accumulatorFunction);
            accumulatorFunctions[index] = accumulatorFunction;
            ++index;
        }
        int slotOriginalSize = input.schema().getFields().size();
        int slotInc = slotOriginalSize;
        Map<Integer, Map<InnerType, Integer>> indexToTypeMap = new HashMap<>();
        List<Field> fieldList = input.schema().getFields();
        for (AccumulatorFunction accumulatorFunction : accumulatorFunctions) {
            InnerType aggInputType = accumulatorFunction.getInputType();
            InnerType aggOutputType = accumulatorFunction.getOutputType();
            int aggInputIndex = accumulatorFunction.getInputColumnIndex();
            InnerType aggInputSourceType = InnerType.from(fieldList.get(aggInputIndex).getType());
            Function column = ExecuteCompiler.column(aggInputIndex, input.schema());
            Map<InnerType, Integer> indexSlot = indexToTypeMap
                    .computeIfAbsent(aggInputIndex, integer -> {
                        HashMap<InnerType, Integer> map = new HashMap<>();
                        map.put(aggInputSourceType, integer);
                        return map;
                    });
            if (aggInputType != null) {
                if (!indexSlot.containsValue(aggInputIndex)) {
                    indexSlot.put(aggInputType, aggInputIndex);
                } else {
                    accumulatorFunction.setInputColumnIndex(slotInc);
                    indexSlot.put(aggInputType, slotInc);
                    slotInc++;
                }
            }
        }
        Function[] exprs = new Function[slotInc];
        for (int i = 0; i < slotOriginalSize; i++) {
            exprs[i] = ExecuteCompiler.column(i, input.schema());
        }
        if (slotInc > slotOriginalSize) {
            for (Map.Entry<Integer, Map<InnerType, Integer>> e1 : indexToTypeMap.entrySet()) {
                for (Map.Entry<InnerType, Integer> e2 : e1.getValue().entrySet()) {
                    Integer inIndex = e1.getKey();
                    Integer outIndex = e2.getValue();
                    if (exprs[outIndex] == null) {
                        exprs[outIndex] = (ExecuteCompiler.cast(ExecuteCompiler.column(inIndex, input.schema()), e2.getKey()));
                    }
                }
            }
        }

        input = executeCompiler.project(input, exprs);

        return executeCompiler.agg(input, ExecuteCompiler.AggImpl.HASH,Arrays.asList(groupSets), Arrays.asList(accumulatorFunctions));

    }

    public PhysicalPlan convertJoin(Join join) {
        JoinRelType joinType = join.getJoinType();
        PhysicalPlan left = convert(join.getLeft());
        PhysicalPlan right = convert(join.getRight());
        Schema createjoinSchema = executeCompiler.createJoinSchema(left, right);
        return executeCompiler.crossJoin(
                left,
                right,
                JoinType.valueOf(joinType.name()),
                ExecuteCompiler.JoinImpl.HASH,
                rexConverter.convertRex(join.getCondition(), createjoinSchema));
    }

    public PhysicalPlan convertProject(Project project) {
        PhysicalPlan input = convert(project.getInput());
        List<RexNode> projects = project.getProjects();

        int index = 0;
        Function[] functions = new Function[projects.size()];
        for (RexNode rexNode : projects) {
            Function function = rexConverter.convertRex(rexNode, input.schema());
            functions[index] = function;
            index++;
        }
        return executeCompiler.project(input, functions);
    }

    public PhysicalPlan convertFilter(Filter filter) {
        PhysicalPlan input = convert(filter.getInput());
        RexNode condition = filter.getCondition();
        Function function = rexConverter.convertRex(condition, input.schema());
        return executeCompiler.filter(input, function);
    }

    public PhysicalPlan convertValues(Values values) {
        ImmutableList<ImmutableList<RexLiteral>> tuples = values.getTuples();

        ArrayList<Function[]> rowList = new ArrayList<>();
        for (ImmutableList<RexLiteral> tuple : tuples) {
            int size = tuple.size();
            Function[] functions = new Function[size];
            int index = 0;
            for (RexLiteral rexLiteral : tuple) {
                Function function = rexConverter.convertToFunction(rexLiteral);
                functions[index] = function;
                index++;
            }
            rowList.add(functions);
        }
        RelDataType rowType = values.getRowType();
        MycatRelDataType mycatRelDataType = MycatRelDataTypeUtil.getMycatRelDataType(rowType);
        Schema schema = FactoryUtil.toArrowSchema(mycatRelDataType);
        return executeCompiler.values(rowList,schema);
    }
}
