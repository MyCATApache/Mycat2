package io.mycat.calcite;

import java.util.Arrays;
import java.util.function.Function;

public class DataMappingEvaluator {
    private final String[] values;
    private final RowSignature rowSignature;
    private final Function<String[], int[]> function;
    private static final int[] EMPTY = new int[]{};

    public DataMappingEvaluator(RowSignature rowSignature, Function<String[], int[]> function) {
        this.values = new String[rowSignature.getColumnCount()];
        this.rowSignature = rowSignature;
        this.function = function;

    }

    public DataMappingEvaluator(RowSignature rowSignature) {
        this(rowSignature, (v) -> EMPTY);
    }

    void assignment(int index, String value) {
        values[index] = value;
    }

    public int[] calculate() {
        try {
            return function.apply(values);
        }finally {
            Arrays.fill(values,null);
        }
    }
}