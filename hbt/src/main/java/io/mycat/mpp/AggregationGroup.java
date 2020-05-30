package io.mycat.mpp;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class AggregationGroup {
    final AggregationCallExp[] columns;

    public final static AggregationGroup of(AggregationCallExp[] columns) {
        return new AggregationGroup(columns);
    }

    public final static AggregationGroup of(String[] aggCallNames,
                                            String[] columnNames,
                                            List<List<Integer>> args) {
        int length = aggCallNames.length;
        AggregationCallExp[] columns = new AggregationCallExp[length];
        for (int i = 0; i < length; i++) {
            String aggCallName = aggCallNames[i];
            String columnName = columnNames[i];
            int fieldIndex = args.isEmpty()?-1:0;
            columns[i] = AggregationCallExp.of(fieldIndex,aggCallName);
        }
        return of(columns);
    }

    public AggregationGroup merge(AggregationGroup other) {
        int length = this.columns.length;
        if (length != other.columns.length) {
            throw new AssertionError();
        }
        for (int i = 0; i < length; i++) {
            this.columns[i].merge(other.columns[i]);
        }
        return this;

    }
}