package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.TableHandler;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.hbt4.ShardingInfo;
import io.mycat.router.ShardingTableHandler;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;


public class MycatTable extends MycatLogicTable   {

    public MycatTable(TableHandler t) {
        super(t);
    }
    public PartInfo computeDataNode(RexNode condition) {
        ShardingTableHandler table = (ShardingTableHandler) getTable();
        if (condition.getKind() == SqlKind.EQUALS) {
            RexCall rexNode = (RexCall) condition;
            List<RexNode> operands = new ArrayList<>(rexNode.getOperands());
            RexNode rexNode1 = operands.get(0);
            RexNode rexNode2 = operands.get(1);

            String columnName = null;
            String value = null;
            if (rexNode1 instanceof RexInputRef) {
                int index = ((RexInputRef) rexNode1).getIndex();
                List<String> fieldNames = getRowType().getFieldNames();
                columnName = fieldNames.get(index);
            }
            if (rexNode2 instanceof RexLiteral) {
                value = ((RexLiteral) rexNode2).getValue().toString();
            }
            BiFunction<String, String, Integer> function = new BiFunction<String, String, Integer>() {
                @Override
                public Integer apply(String s, String s2) {
                    return null;
                }
            };
            if (columnName != null && value != null) {
                Integer apply = function.apply(columnName, value);

                PartImpl part = new PartImpl(MessageFormat.format("{0}[{1}]",
                        table.getSchemaName() + "." + table.getTableName(),
                    1));
                return new SinglePartInfo(part);
            }
        }
        return computeDataNode();
    }

    public PartInfo computeDataNode() {
        ShardingTableHandler table = (ShardingTableHandler) getTable();
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();
        int size = table.getShardingBackends().size();
        return new RangePartInfo(schemaName,tableName,0,size);
    }

    public ShardingInfo getShardingInfo() {
        return new ShardingInfo(ImmutableList.of(),ImmutableList.of(),1,"");
    }
}