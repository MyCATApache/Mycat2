package io.mycat.mpp.plan;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

@Builder
@AllArgsConstructor
@Getter
@ToString
public class RowType {
    final Column[] columns;
    public Comparator<DataAccessor> getComparator(int index) {
        return columns[index].createComparator(index);
    }

    public static RowType of(Column... columns){
        return new RowType(columns);
    }

    public RowType join(RowType o){
        ArrayList<Column> columns = new ArrayList<>();
        columns.addAll(Arrays.asList(this.columns));
        columns.addAll(Arrays.asList(o.columns));
        return of(columns.toArray(new Column[0]));
    }
}