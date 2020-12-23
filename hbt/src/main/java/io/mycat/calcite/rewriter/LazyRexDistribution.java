package io.mycat.calcite.rewriter;

import com.google.common.collect.Iterables;
import io.mycat.DataNode;
import io.mycat.calcite.table.AbstractMycatTable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@AllArgsConstructor
@EqualsAndHashCode
public class LazyRexDistribution extends Distribution {
    final AbstractMycatTable tableHandler;
    Function<List<Object>, Iterable<DataNode>> function;
    List<RexNode> conditions;

    public static LazyRexDistribution of(AbstractMycatTable tableHandler, List<RexNode> conditions,
                                         Function<List<Object>, Iterable<DataNode>> function) {
        return new LazyRexDistribution(tableHandler, function, conditions);
    }

    @Override
    public Iterable<DataNode> getDataNodes(List<Object> params) {
        return function.apply(params);
    }

    @Override
    public Iterable<DataNode> getDataNodes() {
        return () -> tableHandler.computeDataNode().getDataNodes().iterator();
    }

    @Override
    public boolean isSingle() {
        return tableHandler.isSingle(conditions);
    }

    @Override
    public boolean isBroadCast() {
        return tableHandler.isBroadCast();
    }

    @Override
    public boolean isSharding() {
        return tableHandler.isSharding();
    }


    @Override
    public boolean isPartial() {
        return tableHandler.isPartial(conditions);
    }

    @Override
    public Type type() {
        return tableHandler.isNormal() ? Type.PHY : tableHandler.isSharding() ? Type.Sharding : tableHandler.isBroadCast() ? Type.BroadCast : Type.PHY;
    }

    @Override
    public boolean isPhy() {
        return tableHandler.isNormal();
    }

    @Override
    public String toString() {
        return Collections.singletonMap("dataNodes", Iterables.toString(getDataNodes())).toString();
    }
}