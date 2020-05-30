package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLAggregateOption;
import io.mycat.mpp.plan.DataAccessor;
import io.mycat.mpp.runtime.Type;

public class AggregationCallExp implements SqlValue {
    private final int fieldIndex;
    private final AggCalls.AggCall call;
    private final SQLAggregateOption option = SQLAggregateOption.ALL;

    public AggregationCallExp(int fieldIndex, AggCalls.AggCall call) {
        this.fieldIndex = fieldIndex;
        this.call = call;
    }

    public static AggregationCallExp of( int fieldIndex, String aggCallName) {
        return of( fieldIndex, AggCalls.getAggCall(aggCallName));
    }

    public static AggregationCallExp of(int fieldIndex, AggCalls.AggCall call) {
        return new AggregationCallExp( fieldIndex, call);
    }




    public void accept(DataAccessor tuple) {
        if (fieldIndex < 0) {
            call.accept(null);
        } else {
            Object o = tuple.get(fieldIndex);
            call.accept(o);
        }
    }


    public Object getValue() {
        return call.getValue();
    }


    public void reset() {
        call.reset();
    }


    public Class type() {
        return call.type();
    }

    @Override
    public SQLObject toParseTree() {
        return null;
    }

    public void merge(AggregationCallExp column) {
        call.merge(column.call);
    }

    @Override
    public Type getType() {
        return null;
    }
}