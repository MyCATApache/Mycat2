package io.ordinate.engine.factory;

import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.TableHandler;
import io.mycat.prototypeserver.mysql.VisualTableHandler;
import io.ordinate.engine.builder.CorrelationKey;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.bind.VariableParameterFunction;
import io.reactivex.rxjava3.core.Observable;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ComplierContext {
    public void put(String name, Object value) {

    }

    public Function convertRex(RexNode rexNode) {
        if (rexNode instanceof RexDynamicParam) {
            return visitDynamicParam((RexDynamicParam) rexNode);
        } else if (rexNode instanceof RexInputRef) {
            return convertRexInputRefToFunction((RexInputRef) rexNode);
        } else if (rexNode instanceof RexLiteral) {
            return convertToFunction((RexLiteral) rexNode);
        } else if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            List<Function> childrens = rexCall.getOperands().stream().map(i -> convertRex(i)).collect(Collectors.toList());
            return convertToFunction(rexCall, childrens);
        } else if (rexNode instanceof RexCorrelVariable) {
            throw new UnsupportedOperationException();
        } else if (rexNode instanceof RexFieldAccess) {
            RexFieldAccess rexFieldAccess = (RexFieldAccess) rexNode;
            RexCorrelVariable rexCorrelVariable = (RexCorrelVariable) rexFieldAccess.getReferenceExpr();
            int index = rexFieldAccess.getField().getIndex();
            CorrelationId id = rexCorrelVariable.id;

            CorrelationKey correlationKey = new CorrelationKey();
            correlationKey.correlationId = id;
            correlationKey.index = index;
            return null;
        }
        throw new UnsupportedOperationException();
    }

    private Function convertToFunction(RexCall rexCall, List<Function> childrens) {
        return null;
    }

    private Function convertToFunction(RexLiteral rexNode) {
        return null;
    }

    private Function convertRexInputRefToFunction(RexInputRef rexNode) {
        return null;
    }

    private Function visitDynamicParam(RexDynamicParam rexNode) {
        return null;
    }


    public Observable<Object[]> getTableObservable(String schemaName, String tableName) {
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        TableHandler tableHandler = metadataManager.getTable(schemaName, tableName);
        VisualTableHandler visualTableHandler = (VisualTableHandler) tableHandler;
        return visualTableHandler.scanAll();
    }
}
