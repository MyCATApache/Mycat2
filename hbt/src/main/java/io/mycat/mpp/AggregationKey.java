package io.mycat.mpp;

import io.mycat.mpp.plan.DataAccessor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@AllArgsConstructor
@Getter
public class AggregationKey {
    final Object[] values;

    public final static AggregationKey of(Object[] values) {
        return new AggregationKey(values);
    }

    public final static AggregationKey of(DataAccessor row, int[] groupIndexes) {
        int length = groupIndexes.length;
        Object[] values = new Object[length];
        for (int i = 0; i < length; i++) {
            values[i] = row.get(groupIndexes[i]);
        }
        return of(values);
    }
}