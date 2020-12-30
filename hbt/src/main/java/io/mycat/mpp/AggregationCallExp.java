package io.mycat.mpp;

import com.alibaba.druid.sql.ast.SQLObject;
import io.mycat.mpp.plan.DataAccessor;

public class AggregationCallExp implements ASTExp {
    private final String columnName;
    private final int fieldIndex;
    private final AggCalls.AggCall call;

    public AggregationCallExp(String columnName, int fieldIndex, AggCalls.AggCall call) {
        this.columnName = columnName;
        this.fieldIndex = fieldIndex;
        this.call = call;
    }

    public static AggregationCallExp of(String columnName, int fieldIndex, String aggCallName) {
        return of(columnName, fieldIndex, AggCalls.getAggCall(aggCallName));
    }

    public static AggregationCallExp of(String columnName, int fieldIndex, AggCalls.AggCall call) {
        return new AggregationCallExp(columnName, fieldIndex, call);
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
}