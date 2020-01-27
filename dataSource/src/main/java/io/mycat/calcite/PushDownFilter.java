package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.QueryBackendTask;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.relBuilder.MyRelBuilder;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class PushDownFilter extends RelOptRule {

    public PushDownFilter(RelOptRuleOperand operand, String description) {
        super(operand, "Push_down_rule:" + description);
    }

    //   operand(LogicalTableScan.class, none())
    public static final PushDownFilter PROJECT_ON_FILTER2 =
            new PushDownFilter(operand(Bindables.BindableTableScan.class, any()), "proj on filter on proj");
    public static final PushDownFilter PROJECT_ON_FILTER =
            new PushDownFilter(
                    operand(LogicalProject.class,
                            operand(LogicalFilter.class,
                                    operand(LogicalProject.class,
                                            operand(LogicalTableScan.class, none())))), "proj on filter on proj");

    public static final PushDownFilter FILTER_ON_PROJECT =
            new PushDownFilter(
                    operand(LogicalFilter.class,
                            operand(LogicalProject.class,
                                    operand(LogicalTableScan.class, none()))), "filter on proj");

    public static final PushDownFilter FILTER =
            new PushDownFilter(
                    operand(LogicalFilter.class,
                            operand(LogicalTableScan.class, none())), "filter");

    public static final PushDownFilter PROJECT =
            new PushDownFilter(
                    operand(LogicalProject.class,
                            operand(LogicalTableScan.class, none())), "proj");

    /**
     *
     * @param call
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        Bindables.BindableTableScan bindableTableScan = (Bindables.BindableTableScan) call.rels[0];
        RelOptCluster cluster = bindableTableScan.getCluster();//工具类
        RelOptTable relOptTable = bindableTableScan.getTable();//包装表
        RelOptSchema relOptSchema = bindableTableScan.getTable().getRelOptSchema();//schema信息
        RelBuilder builder = call.builder();//关系表达式构建工具
        MycatLogicTable logicTable = relOptTable.unwrap(MycatLogicTable.class);

        if (logicTable != null) {
            ImmutableList<RexNode> filters = bindableTableScan.filters == null ? ImmutableList.of() : bindableTableScan.filters;
            List<Integer> list = bindableTableScan.projects == null ? ImmutableList.of() : bindableTableScan.projects;
            List<QueryBackendTask> queryBackendTasks = CalciteUtls.getQueryBackendTasks(logicTable.logicTable(), new ArrayList<>(filters), list.stream().mapToInt(i -> i).toArray());
                HashMap<String,List<RelNode>> bindTableGroup = new HashMap<>();
                for (QueryBackendTask queryBackendTask : queryBackendTasks) {
                    String targetName = queryBackendTask.getBackendTableInfo().getTargetName();
                    List<RelNode> queryBackendTasksList = bindTableGroup.computeIfAbsent(targetName, (s) -> new ArrayList<>());
                    queryBackendTasksList.add( getBindableTableScan(bindableTableScan, cluster, relOptSchema, queryBackendTask));
                }
                HashMap<String,RelNode> relNodeGroup = new HashMap<>();
                for (Map.Entry<String, List<RelNode>> stringListEntry : bindTableGroup.entrySet()) {
                    builder.pushAll(stringListEntry.getValue());
                    builder.union(true,stringListEntry.getValue().size());
                    relNodeGroup.put(stringListEntry.getKey(),builder.build());
                }
                builder.pushAll(relNodeGroup.values());
                builder.union(true,relNodeGroup.size());

                RelNode build = builder.build();
                DataNodeSqlConverter relToSqlConverter = new DataNodeSqlConverter();
                SqlImplementor.Result visit = relToSqlConverter.visitChild(0,build);
                SqlNode sqlNode = visit.asStatement();
                System.out.println(sqlNode);

                String tableName = String.join(".",relOptTable.getQualifiedName());
            call.transformTo(MyRelBuilder.makeTransientSQLScan(builder,tableName,build));
            // push down filter

        }

    }

    @NotNull
    private RelNode getBindableTableScan(Bindables.BindableTableScan bindableTableScan, RelOptCluster cluster, RelOptSchema relOptSchema, QueryBackendTask queryBackendTask) {
        String uniqueName = queryBackendTask.getBackendTableInfo().getUniqueName();
        RelOptTable dataNodes = relOptSchema.getTableForMember(Arrays.asList(MetadataManager.DATA_NODES, uniqueName));
        RelNode logicalTableScan = LogicalTableScan.create(cluster, dataNodes);
        RelNode project = RelOptUtil.createProject(RelOptUtil.createFilter(logicalTableScan, bindableTableScan.filters), bindableTableScan.projects);
        return project;
    }
}