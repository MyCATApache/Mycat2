package io.mycat.calcite;

import io.mycat.sqlparser.util.complie.RangeVariableType;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class RangeVariable {
    private final int index;
    private boolean or;
    private RangeVariableType operator;
    private Object value;
    private Object optionValue = null;

    public RangeVariable(int index,boolean or, RangeVariableType operator, Object value) {
        this.index  = index;
        this.or = or;
        this.operator = operator;
        assert operator == io.mycat.sqlparser.util.complie.RangeVariableType.EQUAL;
        this.value = value;
    }

    public RangeVariable(int index, boolean or, RangeVariableType range, String begin, String end) {
        this.index  = index;
        this.or = or;
        this.operator = range;
        assert operator == io.mycat.sqlparser.util.complie.RangeVariableType.RANGE;
        this.value = begin;
        this.optionValue = end;
    }

    public RangeVariableType getOperator() {
        return operator;
    }

    public Object getBegin() {
        return value;
    }

    public Object getEnd() {
        return optionValue;
    }


    public boolean isOr() {
        return or;
    }
}