/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import io.mycat.sqlparser.util.complie.RangeVariableType;
import lombok.EqualsAndHashCode;
/**
 * @author Junwen Chen
 **/
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