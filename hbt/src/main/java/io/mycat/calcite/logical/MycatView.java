/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.mycat.*;
import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.*;
import io.mycat.calcite.localrel.*;
import io.mycat.calcite.physical.MycatHashJoin;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.physical.MycatProject;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.IndexCondition;
import io.mycat.calcite.rewriter.PredicateAnalyzer;
import io.mycat.calcite.spm.ParamHolder;
import io.mycat.calcite.table.*;
import io.mycat.config.ServerConfig;
import io.mycat.querycondition.QueryType;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.RxBuiltInMethodImpl;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;


public class MycatView extends AbstractRelNode implements MycatRel {
    final RelNode relNode;
    final Distribution distribution;
    final RexNode condition;
    List<IndexCondition> indexConditions = null;

    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode) {
        this(relTrait, input, dataNode, null);
    }

    public MycatView(RelInput relInput) {
        this(relInput.getTraitSet(), relInput.getInput(), Distribution.fromJson((List) relInput.get("distribution")), relInput.getExpression("condition"));
    }

    public MycatView(RelTraitSet relTrait, RelNode input, Distribution dataNode, RexNode conditions) {
        super(input.getCluster(), relTrait = relTrait.replace(MycatConvention.INSTANCE));
        this.distribution = Objects.requireNonNull(dataNode);
        this.condition = conditions;
        this.rowType = input.getRowType();
//        if (input instanceof MycatRel) {
//            input = input.accept(new ToLocalConverter());
//        }
        ToLocalConverter toLocalConverter = new ToLocalConverter();
        input = input.accept(toLocalConverter);
        if (input instanceof Project) {
            Project project = (Project) input;
            if (RexUtil.isIdentity(project.getProjects(), project.getInput().getRowType())) {
                input = project.getInput();//ProjectRemoveRule
            }
        }
        this.relNode = input;
    }

    public static ProjectIndexMapping project(ShardingIndexTable shardingIndexTable, List<Integer> projects) {
        ArrayList<String> restColumnListBuilder = new ArrayList<>();
        ArrayList<String> indexColumnListBuilder = new ArrayList<>();
        ShardingTable factTable = shardingIndexTable.getFactTable();
        for (int i = 0; i < projects.size(); i++) {
            Integer index = projects.get(i);
            Objects.requireNonNull(index);
            String columnName = factTable.getColumns().get(index).getColumnName();
            boolean covering = shardingIndexTable.getColumnByName(columnName) != null;
            if (covering) {
                indexColumnListBuilder.add(columnName);
            } else {
                restColumnListBuilder.add(columnName);
            }
        }
        List<String> indexEqualKeys = (List) ImmutableList.builder().addAll(shardingIndexTable.getLogicTable().getShardingKeys())
                .add(shardingIndexTable.getFactTable().getPrimaryKey().getColumnName()).build();
        for (String shardingKey : indexEqualKeys) {
            if (!restColumnListBuilder.contains(shardingKey)) {
                restColumnListBuilder.add(shardingKey);
            }
            if (!indexColumnListBuilder.contains(shardingKey)) {
                indexColumnListBuilder.add(shardingKey);
            }
        }
        List<String> indexColumnList = indexColumnListBuilder;
        List<String> restColumnList = restColumnListBuilder;

        return new ProjectIndexMapping(indexColumnListBuilder, restColumnListBuilder);
    }

    public static List<RelNode> produceIndexViews(
            TableScan tableScan,
            final RexNode wholeCondition,
            List<Integer> projects,
            RelDataType orginalRowType) {
        return produceIndexViews(tableScan, wholeCondition, projects, orginalRowType, null);
    }

    public static List<RelNode> produceIndexViews(
            TableScan tableScan,
            final RexNode wholeCondition,
            List<Integer> projects,
            RelDataType orginalRowType, String indexName) {
        DrdsSqlCompiler drdsSqlCompiler = MetaClusterCurrent.wrapper(DrdsSqlCompiler.class);
        RelOptCluster cluster = tableScan.getCluster();
        RelBuilder relBuilder = MycatCalciteSupport.relBuilderFactory.create(cluster, drdsSqlCompiler.getCatalogReader());
        ShardingTable shardingTable = (ShardingTable) tableScan.getTable().unwrap(MycatLogicTable.class).getTable();
        List<ShardingIndexTable> indexTables = shardingTable.getIndexTables();
        String[] shardingKeys = shardingTable.getLogicTable().getShardingKeys().toArray(new String[]{});
        ArrayList<RelNode> tableArrayList = new ArrayList<>(indexTables.size());
        MycatView primaryTableScan;
        for (ShardingIndexTable indexTable : indexTables) {
            if (indexName != null && !indexName.equalsIgnoreCase(indexTable.getIndexName())) {
                continue;
            }
            ProjectIndexMapping indexMapping = project(indexTable, projects);
            boolean indexOnlyScan = !indexMapping.needFactTable();
            final IndexCondition indexCondition;

            if (wholeCondition != null) {
                PredicateAnalyzer predicateAnalyzer = new PredicateAnalyzer(indexTable.keyMetas(), shardingTable.getColumns().stream().map(i -> i.getColumnName()).collect(Collectors.toList()));
                Map<QueryType, List<IndexCondition>> queryTypeListMap = predicateAnalyzer.translateMatch(wholeCondition);
                if (queryTypeListMap.isEmpty()) return Collections.emptyList();
                List<IndexCondition> next = queryTypeListMap.values().stream().filter(i -> i != null).iterator().next().stream()
                        .filter(i -> i != null).collect(Collectors.toList());
                indexCondition = next.get(0);
                primaryTableScan = MycatView
                        .ofCondition(
                                LocalFilter.create(wholeCondition, LocalTableScan.create((TableScan) relBuilder.scan(shardingTable.getSchemaName(), shardingTable.getTableName()).build())),
                                Distribution.of(shardingTable),
                                wholeCondition
                        );
            } else {
                continue;
            }

            RelNode indexTableView = null;
            LocalFilter localFilter = null;
            RexNode pushdownCondition = null;
            LocalTableScan localTableScan = LocalTableScan.create(
                    (TableScan) relBuilder.scan(indexTable.getSchemaName(), indexTable.getTableName()).build());
            switch (indexCondition.getQueryType()) {
                case PK_POINT_QUERY:
                    List<String> indexColumnNames = indexCondition.getIndexColumnNames();
                    RexNode rexNode = indexCondition.getPushDownRexNodeList().get(0);
                    relBuilder.push(localTableScan);

                    List<RexNode> pushDownConditions = new ArrayList<>();
                    for (String indexColumnName : indexColumnNames) {
                        pushDownConditions.add(relBuilder.equals(relBuilder.field(indexColumnName), rexNode));

                    }

                    pushdownCondition = RexUtil.composeConjunction(relBuilder.getRexBuilder(), pushDownConditions);
                    RelNode build = relBuilder.build();
                    localFilter = LocalFilter.create(LogicalFilter.create(localTableScan, pushdownCondition), localTableScan);
                    indexTableView = localFilter;

                    break;
                case PK_RANGE_QUERY:
                case PK_FULL_SCAN:
                    continue;
            }

            if (indexOnlyScan) {
                List<Integer> newProject = getProjectIntList(projects, tableScan.deriveRowType(), indexTableView.getRowType());
                MycatView view = MycatView
                        .ofCondition(LocalProject.create((Project) RelOptUtil.createProject(indexTableView,
                                        newProject), indexTableView),
                                Distribution.of(indexTable), pushdownCondition);
                tableArrayList.add(view);
                continue;
            } else {
                indexTableView = MycatView
                        .ofCondition(indexTableView,
                                Distribution.of(indexTable), pushdownCondition);
                RelNode leftProject = createMycatProject(indexTableView, indexMapping.getIndexColumns());
                RelNode rightProject = createMycatProject(primaryTableScan, indexMapping.getFactColumns());


                Join relNode = (Join) relBuilder

                        .push(leftProject)
                        .push(rightProject)

                        //依赖TableLookupJoin优化
                        .join(JoinRelType.INNER, shardingKeys).build();

                relNode = MycatHashJoin.create(relNode.getTraitSet(), ImmutableList.of(), leftProject, rightProject, relNode.getCondition(), relNode.getJoinType());

                MycatRel mycatProject = createMycatProject(relNode, getProjectStringList(projects, tableScan.getRowType()));

                if (RelOptUtil.areRowTypesEqual(orginalRowType, mycatProject.getRowType(), false)) {
                    tableArrayList.add(mycatProject);
                }
                continue;
            }
        }
        return (List) tableArrayList;
    }

    @NotNull
    private static List<Integer> getProjectIntList(List<Integer> projects, RelDataType orginalRowType, RelDataType indexTableScanRowType) {
        ArrayList<Integer> newProject = new ArrayList<>();
        for (int project : projects) {
            String name = orginalRowType.getFieldList().get(project).getName();
            RelDataTypeField field = indexTableScanRowType.getField(name, false, false);
            if (field == null) {
                throw new IllegalArgumentException("can not find field:" + name);
            } else {
                int index = field.getIndex();
                newProject.add(index);
            }

        }
        return newProject;
    }

    @NotNull
    public static List<String> getProjectStringList(List<Integer> projects, RelDataType orginalRowType) {
        ArrayList<String> newProject = new ArrayList<>();
        for (int project : projects) {
            String name = orginalRowType.getFieldList().get(project).getName();
            newProject.add(name);
        }
        return newProject;
    }

    public static MycatRel createMycatProject(RelNode indexTableScan, List<String> indexColumns) {
        return createMycatProject(indexTableScan, indexColumns, true);
    }

    public static MycatRel createMycatProject(RelNode indexTableScan, List<String> indexColumns, boolean nullable) {
        RelDataType rowType = indexTableScan.getRowType();
        ArrayList<Integer> ints = new ArrayList<>();
        for (String indexColumn : indexColumns) {
            ints.add(rowType.getField(indexColumn, false, false).getIndex());
        }

        RelNode project = RelOptUtil.createProject(indexTableScan, ints);
        if (project instanceof LogicalProject) {
            List<RexNode> projects = ((Project) project).getProjects();
//            if (nullable){
//                RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
//                for (RexNode rexNode : projects) {
//                    rexBuilder.makeNotNull(rexNode);
//                }
//
//            }
            project = MycatProject.create(project.getInput(0), projects, project.getRowType());
        }
        MycatProject mycatProject = (MycatProject) project;
        if (mycatProject.getInput() instanceof MycatView) {
            MycatView mycatProjectInput = (MycatView) mycatProject.getInput();
            return mycatProjectInput.changeTo(mycatProject.copy(mycatProject.getTraitSet(), ImmutableList.of(mycatProjectInput.getRelNode())));
        }

        return (MycatRel) project;
    }

    public List<IndexCondition> getPredicateIndexCondition() {

        if (indexConditions != null) {
            return indexConditions;
        }
        if (this.distribution.getShardingTables().isEmpty() || condition == null) {
            return Collections.emptyList();
        }
        ShardingTable shardingTable = this.distribution.getShardingTables().get(0);
        PredicateAnalyzer predicateAnalyzer = new PredicateAnalyzer(shardingTable.keyMetas(), shardingTable.getLogicTable().getFieldNames());
        Map<QueryType, List<IndexCondition>> queryTypeListMap = predicateAnalyzer.translateMatch(condition);
        indexConditions = ImmutableList.copyOf(queryTypeListMap.values().stream().flatMap(i -> i.stream()).sorted().collect(Collectors.toList()));
        if (indexConditions.isEmpty()) {
            indexConditions = Collections.emptyList();
            return indexConditions;
        }
        return indexConditions;
    }

    public static MycatView ofCondition(RelNode input,
                                        Distribution dataNodeInfo,
                                        RexNode conditions) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, conditions);
    }

    public MycatView changeTo(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo, this.condition);
    }

    public MycatView changeTo(RelNode input) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, distribution, this.condition);
    }

    public static MycatView ofBottom(RelNode input, Distribution dataNodeInfo) {
        return new MycatView(input.getTraitSet().replace(MycatConvention.INSTANCE), input, dataNodeInfo);
    }


    static class FindOrder extends RelShuttleImpl {
        boolean containsOrder = false;

        @Override
        protected RelNode visitChildren(RelNode rel) {
            if (rel instanceof Sort) {
                containsOrder = true;
            }
            return super.visitChildren(rel);
        }

    }


    public SqlNode getSQLTemplate(boolean update) {
        Partition partition;
        if (distribution.type() == Distribution.Type.BROADCAST) {
            GlobalTable globalTable = distribution.getGlobalTables().get(0);
            List<Partition> globalPartition = globalTable.getGlobalDataNode();
            partition = globalPartition.get(0);
        } else if (distribution.type() == Distribution.Type.PHY) {
            partition = distribution.getNormalTables().get(0).getDataNode();
        } else {
            ShardingTable shardingTable = distribution.getShardingTables().get(0);
            partition = shardingTable.dataNodes().get(0);
        }
        String targetName = partition.getTargetName();
        SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
        return MycatCalciteSupport.INSTANCE.convertToSqlTemplate(relNode, dialect, update);
    }

    public ImmutableMultimap<String, SqlString> apply(
            int mergeUnionSize,
            SqlNode sqlTemplateArg,
            List<PartitionGroup> dataNodes, List<Object> params) {
        SqlNode sqlTemplate = sqlTemplateArg;
        if (distribution.type() == Distribution.Type.BROADCAST) {
            GlobalTable globalTable = distribution.getGlobalTables().get(0);
            List<Partition> globalPartition = globalTable.getGlobalDataNode();
            int i = ThreadLocalRandom.current().nextInt(0, globalPartition.size());
            Partition partition = globalPartition.get(i);
            String targetName = partition.getTargetName();
            PartitionGroup nodeMap = dataNodes.get(0);
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            SqlNode sqlSelectStatement = MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params, nodeMap);
            return (ImmutableMultimap.of(targetName, sqlSelectStatement.toSqlString(dialect)));
        }
        if (mergeUnionSize < 1 || isMergeSort()) {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            dataNodes.forEach(m -> {
                String targetName = m.getTargetName();
                SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
                SqlString sqlString = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params, m), (dialect));
                builder.put(targetName, sqlString);
            });
            return (builder.build());
        }
        Map<String, List<Map<String, Partition>>> collect = new HashMap<>();
        for (PartitionGroup m : dataNodes) {
            collect.computeIfAbsent(m.getTargetName(), k -> new ArrayList<>())
                    .add(m.getMap());
        }
        ImmutableMultimap.Builder<String, SqlString> resMapBuilder = ImmutableMultimap.builder();
        for (Map.Entry<String, List<Map<String, Partition>>> entry : collect.entrySet()) {
            String targetName = entry.getKey();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            Iterator<List<Map<String, Partition>>> iterator = Iterables.partition(entry.getValue(), mergeUnionSize + 1).iterator();
            while (iterator.hasNext()) {
                List<Map<String, Partition>> eachList = iterator.next();
                ImmutableList.Builder<SqlString> builderList = ImmutableList.builder();
                SqlString string = null;
                List<Integer> list = new ArrayList<>();
                for (Map<String, Partition> each : eachList) {
                    string = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, params,
                            new PartitionGroup(targetName, each)), dialect);
                    if (string.getDynamicParameters() != null) {
                        list.addAll(string.getDynamicParameters());
                    }
                    builderList.add(string);
                }
                ImmutableList<SqlString> relNodes = builderList.build();
                resMapBuilder.put(targetName,
                        new SqlString(dialect,
                                relNodes.stream().map(i -> i.getSql()).collect(Collectors.joining(" union all ")),
                                ImmutableList.copyOf(list)));
            }
        }
        return (resMapBuilder.build());
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public Distribution getDistribution() {
        return distribution;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatView(traitSet, relNode, distribution);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RelWriter writer = super.explainTerms(pw);
        writer.item("relNode", relNode);
        writer.item("distribution", distribution.toNameList());
        if (condition != null) {
            writer.item("conditions", condition);
        }
        if (isMergeSort()) {
            writer.item("mergeSort", isMergeSort());
        }
        return writer;
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql", getSql(MycatSqlDialect.DEFAULT)).ret();
    }

    public String getSql() {
        return getSql(MycatSqlDialect.DEFAULT).toString();
    }

    public SqlNode getSql(SqlDialect dialect) {
        return MycatCalciteSupport.INSTANCE.convertToSqlTemplate(relNode, dialect, false);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        if (relNode instanceof Sort) {
            Sort relNode = (Sort) this.relNode;
            ParamHolder paramHolder = ParamHolder.CURRENT_THREAD_LOCAL.get();
            List<Object> params = paramHolder.getParams();
            if (params != null && !params.isEmpty()) {
                RexNode fetch = relNode.fetch;
                RexNode offset = relNode.offset;
                Long fetchValue = null;
                if (fetch != null && fetch instanceof RexCall && fetch.isA(SqlKind.PLUS)) {
                    List<RexNode> operands = ((RexCall) fetch).getOperands();
                    Long one = resolveParam(params, operands.get(0));
                    Long two = resolveParam(params, operands.get(1));
                    if (one != null && two != null) {
                        fetchValue = one + two;
                    }
                } else if (fetch instanceof RexLiteral) {
                    fetchValue = resolveParam(params, fetch);
                }

                Long offsetValue = resolveParam(params, offset);
                if (offsetValue == null && fetchValue != null) return fetchValue;
                if (offsetValue != null && fetchValue != null) return fetchValue - offsetValue;
            }
        }
        List<IndexCondition> conditionOptional = getPredicateIndexCondition();
        double v = relNode.estimateRowCount(mq);
        if (!conditionOptional.isEmpty()) {
            IndexCondition indexCondition = conditionOptional.get(0);
            QueryType queryType = indexCondition.getQueryType();
            double factor = queryType.factor();
            switch (queryType) {
                case PK_POINT_QUERY:
                    if (v > 1000) {
                        return 1000;
                    } else {
                        return v;
                    }
                case PK_RANGE_QUERY:
                    return factor * v;
                case PK_FULL_SCAN:
                    return factor * v;
            }
        }
        return v;
    }

    private Long resolveParam(List<Object> params, RexNode fetch) {
        if (fetch != null && fetch instanceof RexDynamicParam) {
            int index = ((RexDynamicParam) fetch).getIndex();
            if (index < params.size()) {
                long l = ((Number) params.get(index)).longValue();
                return l;
            }
        }
        return null;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double v = estimateRowCount(mq);
        RelOptCostFactory costFactory = planner.getCostFactory();
        switch (distribution.type()) {
            case PHY:
            case BROADCAST:
                break;
            case SHARDING:
                List<IndexCondition> predicateIndexConditionOptional = getPredicateIndexCondition();
                if (!predicateIndexConditionOptional.isEmpty()) {
                    IndexCondition indexCondition = predicateIndexConditionOptional.get(0);
                    switch (indexCondition.getQueryType()) {
                        case PK_POINT_QUERY:
                            v = 1;
                            break;
                        case PK_RANGE_QUERY:
                            v = v * 0.5;
                            break;
                        case PK_FULL_SCAN:
                            break;
                    }
                }
                break;
        }
        RelOptCost relOptCost = costFactory.makeCost(v, 0, 0);
        return relOptCost;
    }

    private RelNode applyDataNode(Map<String, Partition> map, RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable mycatLogicTable = scan.getTable().unwrap(MycatLogicTable.class);
                if (mycatLogicTable != null) {
                    String uniqueName = mycatLogicTable.getTable().getUniqueName();
                    Partition partition = map.get(uniqueName);
                    MycatPhysicalTable physicalTable = new MycatPhysicalTable(mycatLogicTable, partition);
                    RelOptTableImpl relOptTable1 = RelOptTableImpl.create(scan.getTable().getRelOptSchema(),
                            scan.getRowType(),
                            physicalTable,
                            ImmutableList.of(partition.getTargetName(), partition.getSchema(), partition.getTable())
                    );
                    return LogicalTableScan.create(scan.getCluster(), relOptTable1, ImmutableList.of());
                }
                return super.visit(scan);
            }
        });
    }


    public Result implementView(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(getDigest());
        Method getObservable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class);
        builder.add(Expressions.call(root, getObservable, mycatViewStash));
        return implementor.result(physType, builder.toBlock());
    }


    public static Expression toEnumerable(Expression expression) {
        final Type type = expression.getType();
        if (Types.isArray(type)) {
            if (Types.toClass(type).getComponentType().isPrimitive()) {
                expression =
                        Expressions.call(BuiltInMethod.AS_LIST.method, expression);
            }
            return Expressions.call(BuiltInMethod.AS_ENUMERABLE.method, expression);
        } else if (Types.isAssignableFrom(Iterable.class, type)
                && !Types.isAssignableFrom(Enumerable.class, type)) {
            return Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method,
                    expression);
        } else if (Types.isAssignableFrom(Queryable.class, type)) {
            // Queryable extends Enumerable, but it's too "clever", so we call
            // Queryable.asEnumerable so that operations such as take(int) will be
            // evaluated directly.
            return Expressions.call(expression,
                    BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
        }
        return expression;
    }

    public static Expression toRows(PhysType physType, Expression expression, final int fieldCount) {
        JavaRowFormat oldFormat = JavaRowFormat.ARRAY;
        if (physType.getFormat() == oldFormat) {
            return expression;
        }
        final ParameterExpression row_ =
                Expressions.parameter(Object[].class, "row");
        List<Expression> expressionList = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            expressionList.add(fieldExpression(row_, i, physType, oldFormat));
        }
        return Expressions.call(expression,
                BuiltInMethod.SELECT.method,
                Expressions.lambda(Function1.class, physType.record(expressionList),
                        row_));
    }

    public static Expression fieldExpression(ParameterExpression row_, int i,
                                             PhysType physType, JavaRowFormat format) {
        return format.field(row_, i, null, physType.getJavaFieldType(i));
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }


    public Result implementViewStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(getDigest());
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class);
        final Expression expression2 = Expressions.call(root, getEnumerable, mycatViewStash);
        builder.add(toRows(physType, expression2, getRowType().getFieldCount()));
        return implementor.result(physType, builder.toBlock());
    }

    public Optional<RexNode> getCondition() {
        return Optional.ofNullable(condition);
    }

    public Result implementMergeSort(MycatEnumerableRelImplementor implementor, Prefer pref, RelNode relNode) {
        MycatMergeSort mycatMergeSort = null;
        MycatView view = (MycatView) relNode;
        if (view.getDistribution().type() == Distribution.Type.SHARDING) {
            if (view.getRelNode() instanceof Sort) {
                Sort viewRelNode = (Sort) view.getRelNode();
                RexNode rexNode = (RexNode) viewRelNode.fetch;
                if (rexNode != null && rexNode.getKind() == SqlKind.PLUS) {
                    RexCall plus = (RexCall) rexNode;
                    mycatMergeSort = MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), plus.getOperands().get(0), plus.getOperands().get(1));
                } else {
                    mycatMergeSort = MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), viewRelNode.offset, viewRelNode.fetch);
                }
            }
        } else {
            throw new IllegalArgumentException();
        }
//            MycatView view = (MycatView) relNode;
//            if (view.getDistribution().type() == Distribution.Type.Sharding) {
//                if (view.getRelNode() instanceof LogicalSort) {
//                    LogicalSort viewRelNode = (LogicalSort) view.getRelNode();
//                    RexNode rexNode = (RexNode) viewRelNode.fetch;
//                    if (rexNode != null && rexNode.getKind() == SqlKind.PLUS) {
//                        RexCall plus = (RexCall) rexNode;
//                        return MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), plus.getOperands().get(0), plus.getOperands().get(1));
//                    } else {
//                        return MycatMergeSort.create(viewRelNode.getTraitSet(), relNode, viewRelNode.getCollation(), viewRelNode.offset, viewRelNode.fetch);
//                    }
//                }
//            }
//        }

        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(relNode.getDigest());

        final PhysType inputPhysType = physType;
        final Pair<Expression, Expression> pair =
                inputPhysType.generateCollationKey(mycatMergeSort.collation.getFieldCollations());

        final Expression fetchVal;
        if (mycatMergeSort.fetch == null) {
            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
        } else {
            fetchVal = getExpression(mycatMergeSort.fetch);
        }
//        builder.append("keySelector", pair.left))
//                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))

        final Expression offsetVal = mycatMergeSort.offset == null ? Expressions.constant(Integer.valueOf(0))
                : getExpression(mycatMergeSort.offset);

        Method getObservable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class, Function1.class, Comparator.class, int.class, int.class);
        builder.add(Expressions.call(root, getObservable, mycatViewStash, pair.left, pair.right, offsetVal, fetchVal));
        return implementor.result(physType, builder.toBlock());

    }
//
//    public Result implementMergeSortStream(MycatEnumerableRelImplementor implementor, Prefer pref, MycatMergeSort mycatMergeSort) {
//        final BlockBuilder builder = new BlockBuilder();
//        final PhysType physType =
//                PhysTypeImpl.of(
//                        implementor.getTypeFactory(),
//                        getRowType(),
//                        JavaRowFormat.ARRAY);
//        ParameterExpression root = implementor.getRootExpression();
//        Expression mycatViewStash = Expressions.constant(mycatMergeSort.getDigest());
//
//        final PhysType inputPhysType = physType;
//        final Pair<Expression, Expression> pair =
//                inputPhysType.generateCollationKey(mycatMergeSort.collation.getFieldCollations());
//
//        final Expression fetchVal;
//        if (mycatMergeSort.fetch == null) {
//            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
//        } else {
//            fetchVal = getExpression(mycatMergeSort.fetch);
//        }
////        builder.append("keySelector", pair.left))
////                                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))
//
//        final Expression offsetVal = mycatMergeSort.offset == null ? Expressions.constant(Integer.valueOf(0))
//                : getExpression(mycatMergeSort.offset);
//        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getObservable", String.class, Function1.class, Comparator.class, int.class, int.class);
//        final Expression expression2 = Expressions.call(root, getEnumerable, mycatViewStash, pair.left, pair.right, offsetVal, fetchVal);
//        builder.add(toRows(physType, expression2, getRowType().getFieldCount()));
//
//
//        return implementor.result(physType, builder.toBlock());
//    }

    public boolean isMergeSort() {
        MycatView view = this;
        if (view.getDistribution().type() == Distribution.Type.SHARDING) {
            return (view.getRelNode() instanceof Sort);
        } else {
            return false;
        }
    }

    public boolean isMergeAgg() {
        MycatView view = this;
        if (view.getDistribution().type() == Distribution.Type.SHARDING) {
            return (view.getRelNode() instanceof Aggregate);
        } else {
            return false;
        }
    }


    public Result implementMergeView(MycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatView input = (MycatView) this;
        return input.implementMergeSort(implementor, pref, this);
    }

    public static Expression getExpression(RexNode rexNode) {
        if (rexNode instanceof RexDynamicParam) {
            final RexDynamicParam param = (RexDynamicParam) rexNode;
            return Expressions.convert_(
                    Expressions.call(DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            Expressions.constant("?" + param.getIndex())),
                    Integer.class);
        } else {
            return Expressions.constant(RexLiteral.intValue(rexNode));
        }
    }

    public static <TSource, TKey> io.reactivex.rxjava3.core.Observable<TSource> streamOrderBy(
            List<Observable<TSource>> sources,
            Function1<TSource, TKey> keySelector,
            Comparator<TKey> comparator,
            int offset, int fetch) {

        return RxBuiltInMethodImpl.mergeSort(sources, (o1, o2) -> {
            TKey left = keySelector.apply(o1);
            TKey right = keySelector.apply(o2);
            return comparator.compare(left, right);
        }, offset, fetch);
    }

    public static <TSource, TKey> Enumerable<TSource> orderBy(
            List<Enumerable<TSource>> sources,
            Function1<TSource, TKey> keySelector,
            Comparator<TKey> comparator,
            int offset, int fetch) {
        Enumerable<TSource> tSources = Linq4j.asEnumerable(new Iterable<TSource>() {
            @NotNull
            @Override
            public Iterator<TSource> iterator() {
                List<Iterator<TSource>> list = new ArrayList<>();
                for (Enumerable<TSource> source : sources) {
                    list.add(source.iterator());
                }

                return Iterators.<TSource>mergeSorted(list, (o1, o2) -> {
                    TKey left = keySelector.apply(o1);
                    TKey right = keySelector.apply(o2);
                    return comparator.compare(left, right);
                });
            }
        });
        tSources = EnumerableDefaults.skip(tSources, offset);
        tSources = EnumerableDefaults.take(tSources, fetch);
        return tSources;
    }


    public Result implementMergeViewStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatView input = this;
        return implementMergeSort(implementor, pref, input);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        if (isMergeSort()) {
            return implementMergeView((MycatEnumerableRelImplementor) implementor, pref);
        }
        return implementView((MycatEnumerableRelImplementor) implementor, pref);
    }

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        if (isMergeSort()) {
            return implementMergeViewStream(implementor, pref);
        }
        return implementViewStream(implementor, pref);
    }

    @Override
    public MycatRelDataType getMycatRelDataTypeByCalcite() {
        LocalRel relNode = (LocalRel)getRelNode();
        return relNode.getMycatRelDataType();
    }

}