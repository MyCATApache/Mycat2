package io.mycat.calcite.rewriter;

import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.gsi.GSIService;
import io.mycat.util.CalciteUtls;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.mycat.util.CalciteUtls.unCastWrapper;

public class PredicateAnalyzer {

    public static List<DataNode> analyze(ShardingTable table, List<RexNode> conditions, List<Object> params) {
        List<RexNode> rexNodes = new ArrayList<>();
        for (RexNode condition : conditions) {
            rexNodes.add(condition.accept(new RexShuttle() {
                @Override
                public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                    RexBuilder rexBuilder = MycatCalciteSupport.RexBuilder;
                    Object o = params.get(dynamicParam.getIndex());
                    RelDataType type;
                    RelDataTypeFactory typeFactory = MycatCalciteSupport.TypeFactory;
                    if (o == null) {
                        type = typeFactory.createSqlType(SqlTypeName.NULL);
                    } else {
                        type = typeFactory.createJavaType(o.getClass());
                    }
                    return rexBuilder.makeLiteral(o, type, true);
                }
            }));
        }
        List<DataNode> backendTableInfos = CalciteUtls.getBackendTableInfos(table, rexNodes);

        if (backendTableInfos.size() > 1 && MetaClusterCurrent.exist(GSIService.class)) {
            GSIService gsiService = MetaClusterCurrent.wrapper(GSIService.class);
            if (rexNodes.size() == 1) {
                RexNode rexNode = rexNodes.get(0);
                if (rexNode.getKind() == SqlKind.EQUALS) {
                    RexCall rexNode1 = (RexCall) rexNode;
                    List<RexNode> operands = rexNode1.getOperands();
                    RexNode left = operands.get(0);
                    left = unCastWrapper(left);
                    RexNode right = operands.get(1);
                    right = unCastWrapper(right);
                    int index = ((RexInputRef) left).getIndex();
                    Object value = ((RexLiteral) right).getValue2();
                    Map<String, DataNode> dataNodeMap = backendTableInfos.stream().collect(Collectors.toMap(DataNode::getTargetName, e -> e));
                    Collection<String> dataNodes = gsiService.queryDataNode(
                            table.getSchemaName(),
                            table.getTableName(),
                            index, value);
                    if (dataNodes == null) {
                        return backendTableInfos;
                    }
                    if (dataNodes.isEmpty()) {
                        return new ArrayList<>();
                    }
                    List<DataNode> res = new ArrayList<>();
                    for (String dataNodeKey : dataNodes) {
                        DataNode dataNode = dataNodeMap.get(dataNodeKey);
                        if (dataNode == null) {
                            throw new IllegalStateException("数据源[" + dataNodeKey + "]不存在, 所有数据源=" + dataNodeMap.keySet());
                        }
                        res.add(dataNode);
                    }
                    return res;
                }
            }
        }
        return backendTableInfos;
    }
}
