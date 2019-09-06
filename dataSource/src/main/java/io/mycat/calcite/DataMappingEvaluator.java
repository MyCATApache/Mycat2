package io.mycat.calcite;

import io.mycat.router.RuleAlgorithm;
import io.mycat.sqlparser.util.complie.RangeVariableType;

import java.util.*;

public class DataMappingEvaluator {
    private final Set<RangeVariable>[] values;
    private final RowSignature rowSignature;
    private final List<String> columnNameList;
    private final RuleAlgorithm function;
    private static final int[] EMPTY = new int[]{};

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
            ++index;
        }

        for (int i = 0; i < values.length; i++) {
            values[i] = new HashSet<>();
        }
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

    /**
     * @param index
     * @param value
     * @return isFirst
     */
    boolean assignment(boolean or, int index, String value) {
        boolean empty = values[index].isEmpty();
        values[index].add(new RangeVariable(or, RangeVariableType.EQUAL, value));
        return empty;
    }

    boolean assignmentRange(boolean or, int index, String begin, String end) {
        boolean empty = values[index].isEmpty();
        values[index].add(new RangeVariable(or, RangeVariableType.RANGE, begin, end));
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
                                continue;
                            }
                            res.add(calculate);
                            break;
                        }
                        case RANGE: {
                            int[] calculate = function.calculateRange(begin, end);
                            if (calculate == null || calculate.length == 0) {
                                continue;
                            }
                            for (int i : calculate) {
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
                if(where.length()>0){
                    if (rangeVariable.isOr()) {
                        where.append(" or (");
                    } else {
                        where.append(" and (");
                    }
                }else {
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
}