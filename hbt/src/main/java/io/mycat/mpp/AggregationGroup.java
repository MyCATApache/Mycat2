package io.mycat.mpp;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class AggregationGroup {
    final AggregationExp[] columns;

    public final static AggregationGroup of(AggregationExp[] columns) {
        return new AggregationGroup(columns);
    }

    public final static AggregationGroup of(String[] aggCallNames,
                                            String[] columnNames,
                                            List<List<Integer>> args) {
        int length = aggCallNames.length;
        AggregationExp[] columns = new AggregationExp[length];
        for (int i = 0; i < length; i++) {
            String aggCallName = aggCallNames[i];
            String columnName = columnNames[i];
            List<Integer> argsList = args.get(i);
            columns[i] = AggregationExp.of(aggCallName, columnName, argsList);
        }
        return of(columns);
    }

    public AggregationGroup merge(AggregationGroup other){
        int length = this.columns.length;
        if(length!=other.columns.length){
            throw new AssertionError();
        }
        for (int i = 0; i < length; i++) {
            this.columns[i].merge( other.columns[i]);
        }
        return this;

    }
}