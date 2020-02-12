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
package io.mycat.calcite.metadata;

import lombok.Data;
import lombok.EqualsAndHashCode;
/**
 * @author Junwen Chen
 **/
@EqualsAndHashCode
@Data
public class RangeVariable {
    private final boolean or;
    private final RangeVariableType operator;
    private final Object value;
    private Object optionValue = null;

    public RangeVariable(boolean or, RangeVariableType operator, Object value) {
        this.or = or;
        this.operator = operator;
        assert operator == RangeVariableType.EQUAL;
        this.value = value;
    }

    public RangeVariable(boolean or, RangeVariableType range, String begin, String end) {
        this.or = or;
        this.operator = range;
        assert operator == RangeVariableType.RANGE;
        this.value = begin;
        this.optionValue = end;
    }

    public Object getBegin() {
        return value;
    }

    public Object getEnd() {
        return optionValue;
    }

}