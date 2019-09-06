package io.mycat.calcite;

import io.mycat.sqlparser.util.complie.RangeVariableType;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class RangeVariable {
    private boolean or;
    private RangeVariableType operator;
    private Object value;
    private Object optionValue = null;

    public RangeVariable(boolean or, RangeVariableType operator, Object value) {
        this.or = or;
        assert operator == io.mycat.sqlparser.util.complie.RangeVariableType.EQUAL;
        this.operator = operator;
        this.value = value;
    }

    public RangeVariable(
            boolean or, RangeVariableType operator, Object value, Object optionValue) {
        this.or = or;
        assert operator == io.mycat.sqlparser.util.complie.RangeVariableType.RANGE;
        this.operator = operator;
        this.value = value;
        this.optionValue = optionValue;
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