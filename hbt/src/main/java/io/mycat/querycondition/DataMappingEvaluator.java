/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.querycondition;

import io.mycat.*;
import io.mycat.router.CustomRuleFunction;

import java.util.*;

/**
 * @author Junwen Chen
 **/
public class DataMappingEvaluator {
    private final Map<String, Collection<RangeVariable>> columnMap = new HashMap<>();

    public void assignment(String columnName, Object value) {
        getRangeVariables(columnName).add(new RangeVariable(columnName, RangeVariableType.EQUAL, value));
    }

    public void assignmentRange(String columnName, Object begin, Object end) {
        getRangeVariables(columnName).add(new RangeVariable(columnName, RangeVariableType.RANGE, begin, end));
    }

    private Collection<RangeVariable> getRangeVariables(String columnName) {
        return columnMap.computeIfAbsent(columnName, s -> new HashSet<>());
    }

    public List<Partition> calculate(CustomRuleFunction ruleFunction, Map<String, Collection<RangeVariable>>  values) {
        Objects.requireNonNull(ruleFunction);
        return ruleFunction.calculate(values);
    }

    public void merge(DataMappingEvaluator arg) {
        arg.columnMap.forEach((key, value) -> this.getRangeVariables(key).addAll(value));
    }

    public Map<String, Collection<RangeVariable>> getColumnMap() {
        return columnMap;
    }
}