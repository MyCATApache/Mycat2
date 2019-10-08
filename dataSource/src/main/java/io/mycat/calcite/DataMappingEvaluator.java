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

import io.mycat.router.RuleAlgorithm;
import io.mycat.sqlparser.util.complie.RangeVariableType;

import java.util.*;
/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class DataMappingEvaluator {
    private final Set<RangeVariable>[] values;
    private final RowSignature rowSignature;
    private final List<String> columnNameList;
    private final RuleAlgorithm function;
    private static final int[] EMPTY = new int[]{};
    private final Map<String,Integer> columnMap = new HashMap<>();
    boolean fail = true;

    ///////////////////optional//////////////////////////////
    private final int[] keys;



    public DataMappingEvaluator(RowSignature rowSignature, List<String> columnNameList, RuleAlgorithm function) {
        this.rowSignature = rowSignature;
        this.columnNameList = columnNameList == null ? Collections.emptyList() : columnNameList;
        this.function = function;
        this.values = new Set[rowSignature.getColumnCount()];

        List<String> rowOrder = rowSignature.getRowOrder();

        /////////////////////////////////////////////////////
        this.keys = new int[columnNameList.size()];
        int index = 0;
        for (String s : columnNameList) {
            this.keys[index] = rowOrder.indexOf(s);
            columnMap.put(s,index);
            ++index;
        }

        for (int i = 0; i < values.length; i++) {
            values[i] = new HashSet<>();
        }
    }

    public DataMappingEvaluator copy(){
       return new DataMappingEvaluator(rowSignature, this.getColumnNameList(), this.getFunction());
    }

    public DataMappingEvaluator(RowSignature rowSignature) {
        this(rowSignature, Collections.emptyList(), new RuleAlgorithm() {
            @Override
            public String name() {
                return null;
            }

            @Override
            public int calculate(String columnValue) {
                return -1;
            }

            @Override
            public int[] calculateRange(String beginValue, String endValue) {
                return new int[0];
            }

            @Override
            public int getPartitionNum() {
                return -1;
            }

            @Override
            public void init(Map<String, String> prot, Map<String, String> ranges) {

            }
        });
    }
    boolean assignment(boolean or, String columnName, String value){
        return assignment(or,columnMap.get(columnName),value);
    }
    boolean assignmentRange(boolean or, String columnName, String begin, String end){
        return assignmentRange(or,columnMap.get(columnName),begin,end);
    }
    /**
     * @param index
     * @param value
     * @return isFirst
     */
    boolean assignment(boolean or, int index, String value) {
        boolean empty = values[index].isEmpty();
        values[index].add(new RangeVariable(index, or, RangeVariableType.EQUAL, value));
        return empty;
    }

    boolean assignmentRange(boolean or, int index, String begin, String end) {
        boolean empty = values[index].isEmpty();
        values[index].add(new RangeVariable(index, or, RangeVariableType.RANGE, begin, end));
        return empty;
    }

    public int[] calculate() {
        try {
            Set<Integer> res = new HashSet<>();
            for (int index : this.keys) {
                Set<RangeVariable> value = values[index];
                for (RangeVariable rangeVariable : value) {
                    String begin = Objects.toString(rangeVariable.getBegin());
                    String end = Objects.toString(rangeVariable.getEnd());
                    switch (rangeVariable.getOperator()) {
                        case EQUAL: {
                            int calculate = function.calculate(begin);
                            if (calculate == -1) {
                                return EMPTY;
                            }
                            res.add(calculate);
                            break;
                        }
                        case RANGE: {
                            int[] calculate = function.calculateRange(begin, end);
                            if (calculate == null || calculate.length == 0) {
                                return EMPTY;
                            }
                            for (int i : calculate) {
                                if (i == -1) {
                                    return EMPTY;
                                }
                                res.add(i);
                            }
                            break;
                        }
                    }
                }
            }
            return res.stream().mapToInt(i -> i).toArray();
        } finally {
            for (Set<RangeVariable> value : values) {
                value.clear();
            }
        }
    }

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public String getFilterExpr() {
        StringBuilder where = new StringBuilder("");
        List<String> rowOrder = rowSignature.getRowOrder();
        for (int i = 0; i < values.length; i++) {
            Set<RangeVariable> value = values[i];

            for (RangeVariable rangeVariable : value) {
                if (where.length() > 0) {
                    if (rangeVariable.isOr()) {
                        where.append(" or (");
                    } else {
                        where.append(" and (");
                    }
                } else {
                    where.append("  (");
                }

                String columnName = rowOrder.get(i);
                switch (rangeVariable.getOperator()) {
                    case EQUAL:
                        where.append(columnName).append(" = ").append(rangeVariable.getBegin());
                        break;
                    case RANGE:
                        where.append(columnName).append(" between ").append(rangeVariable.getBegin()).append(" and ").append(rangeVariable.getEnd());
                        break;
                }
                where.append(" ) ");
            }
        }
        return where.toString();
    }


    public RuleAlgorithm getFunction() {
        return function;
    }

    public void add(DataMappingEvaluator dataMappingRule) {
        Set<RangeVariable>[] values = dataMappingRule.values;
        for (int i = 0; i < values.length; i++) {
            Set<RangeVariable> value = values[i];
            if (value != null) {
                this.values[i].addAll(value);
            }
        }
    }
}