package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.BackendTableInfo;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.logic.MycatPhysicalTable;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.*;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.*;


/*
RexSimplify 简化行表达式
SubstitutionVisitor 物化结合
MaterializedViewSubstitutionVisitor
 */
public class PushDownLogicTable extends RelOptRule {

    public PushDownLogicTable(RelOptRuleOperand operand, String description) {
        super(operand, "Push_down_rule:" + description);
    }

    public static final io.mycat.calcite.PushDownLogicTable INSTANCE_FOR_PushDownFilterLogicTable =
            new PushDownLogicTable(operand(Bindables.BindableTableScan.class, any()), "proj on filter on proj");
    /**
     * @param call todo result set with order，backend
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan judgeObject = (TableScan) call.rels[0];
        RelNode value = toPhyTable(call.builder(), judgeObject);
        if (value != null) {
            call.transformTo(value);
        }
    }

    public static RelNode toPhyTable(RelBuilder builder, TableScan judgeObject) {
        Bindables.BindableTableScan bindableTableScan;
        if (judgeObject instanceof Bindables.BindableTableScan) {
            bindableTableScan = (Bindables.BindableTableScan) judgeObject;
        } else {
            bindableTableScan = Bindables.BindableTableScan.create(judgeObject.getCluster(), judgeObject.getTable());
        }
        RelOptCluster cluster = bindableTableScan.getCluster();//工具类
        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        RelOptTable relOptTable = bindableTableScan.getTable();//包装表
        RelOptSchema relOptSchema = bindableTableScan.getTable().getRelOptSchema();//schema信息
        MycatLogicTable logicTable = relOptTable.unwrap(MycatLogicTable.class);

        RelNode value = null;
        if (logicTable != null) {
            ArrayList<RexNode> filters = new ArrayList<>(bindableTableScan.filters == null ? Collections.emptyList() : bindableTableScan.filters);
            List<BackendTableInfo> backendTableInfos = CalciteUtls.getBackendTableInfos(logicTable.logicTable(), filters);

            ////////////////////////////////////////////////////////////////////////////////////////////////
            //视图优化


            ////////////////////////////////////////////////////////////////////////////////////////////////

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
                relNodeGroup.put(targetName, builder.build());
            }

            if (relNodeGroup.size() == 1) {
                value = relNodeGroup.entrySet().iterator().next().getValue();
            } else {
                builder.pushAll(relNodeGroup.values());
                value = builder.union(true, relNodeGroup.size()).build();
            }
        } else {
            value = bindableTableScan;
        }
        return value;
    }

    @NotNull
    private static RelNode getBindableTableScan(Bindables.BindableTableScan bindableTableScan, RelOptCluster cluster, RelOptSchema relOptSchema, BackendTableInfo backendTableInfo) {
        String uniqueName = backendTableInfo.getUniqueName();
        MycatLogicTable unwrap = bindableTableScan.getTable().unwrap(MycatLogicTable.class);
        MycatPhysicalTable mycatPhysicalTable = unwrap.getMycatPhysicalTable(uniqueName);
        RelOptTable dataNode = RelOptTableImpl.create(
                relOptSchema,
                mycatPhysicalTable.getRowType(cluster.getTypeFactory()),
                mycatPhysicalTable,
                ImmutableList.of(uniqueName));
        RelNode logicalTableScan = LogicalTableScan.create(cluster, dataNode);
        return RelOptUtil.createProject(RelOptUtil.createFilter(logicalTableScan, bindableTableScan.filters), bindableTableScan.projects);
    }
}