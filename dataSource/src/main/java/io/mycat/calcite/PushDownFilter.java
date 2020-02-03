package io.mycat.calcite;

import io.mycat.BackendTableInfo;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.relBuilder.MyRelBuilder;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
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
     * @param call
     * todo result set with order，backend
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        Bindables.BindableTableScan bindableTableScan = (Bindables.BindableTableScan) call.rels[0];
        RelOptCluster cluster = bindableTableScan.getCluster();//工具类
        RelOptTable relOptTable = bindableTableScan.getTable();//包装表
        RelOptSchema relOptSchema = bindableTableScan.getTable().getRelOptSchema();//schema信息
        RelBuilder builder = call.builder();//关系表达式构建工具
        MycatLogicTable logicTable = relOptTable.unwrap(MycatLogicTable.class);
        String tableName = String.join(".", relOptTable.getQualifiedName());

        if (logicTable != null) {
            ArrayList<RexNode> filters =new ArrayList<>( bindableTableScan.filters == null ? Collections.emptyList() : bindableTableScan.filters);
            List<BackendTableInfo> backendTableInfos = CalciteUtls.getBackendTableInfos(logicTable.logicTable(), filters);
            HashMap<String, List<RelNode>> bindTableGroupMapByTargetName = new HashMap<>();
            for (BackendTableInfo backendTableInfo : backendTableInfos) {
                String targetName = backendTableInfo.getTargetName();
                List<RelNode> queryBackendTasksList = bindTableGroupMapByTargetName.computeIfAbsent(targetName, (s) -> new ArrayList<>());
                queryBackendTasksList.add(getBindableTableScan(bindableTableScan, cluster, relOptSchema, backendTableInfo));
            }
            HashMap<String, RelNode> relNodeGroup = new HashMap<>();
            for (Map.Entry<String, List<RelNode>> entry : bindTableGroupMapByTargetName.entrySet()) {
                String targetName = entry.getKey();
                builder.pushAll(entry.getValue());
                builder.union(true, entry.getValue().size());
                relNodeGroup.put(targetName, MyRelBuilder.makeTransientSQLScan(builder, targetName, tableName, builder.build()));
            }
            if (relNodeGroup.size() == 1) {
                Map.Entry<String, RelNode> next = relNodeGroup.entrySet().iterator().next();
                call.transformTo(next.getValue());
                return;
            } else {
                builder.pushAll(relNodeGroup.values());
                builder.union(true, relNodeGroup.size());
                call.transformTo(builder.build());
                return;
            }
        }

    }

    @NotNull
    private RelNode getBindableTableScan(Bindables.BindableTableScan bindableTableScan, RelOptCluster cluster, RelOptSchema relOptSchema, BackendTableInfo backendTableInfo) {
        String uniqueName = backendTableInfo.getUniqueName();
        RelOptTable dataNode = relOptSchema.getTableForMember(Arrays.asList(MetadataManager.DATA_NODES, uniqueName));
        RelNode logicalTableScan = LogicalTableScan.create(cluster, dataNode);
        return RelOptUtil.createProject(RelOptUtil.createFilter(logicalTableScan, bindableTableScan.filters), bindableTableScan.projects);
    }
}