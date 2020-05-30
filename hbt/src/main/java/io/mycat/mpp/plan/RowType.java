package io.mycat.mpp.plan;

import io.mycat.mpp.SqlValue;
import io.mycat.mpp.runtime.Type;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Builder
@AllArgsConstructor
@Getter
@ToString
public class RowType {
    final Column[] columns;

    public Comparator<DataAccessor> getComparator(int index) {
        return columns[index].createComparator(index);
    }

    public static RowType of(Column... columns) {
        return new RowType(columns);
    }

    public static RowType of(List<SqlValue> values){
        return of(values,null);
    }
    public static RowType of(List<SqlValue> values, List<String> aliasList) {
        int size = values.size();
        Column[] columns = new Column[size];
        for (int i = 0; i < size; i++) {
            Type type = values.get(i).getType();
            String s;
            if (aliasList == null) {
                s = values.get(i).toParseTree().toString();
            } else {
                s = aliasList.get(i);
            }
            columns[i] = Column.of(s, type);
        }
        return RowType.of(columns);
    }

    public RowType join(RowType o) {
        ArrayList<Column> columns = new ArrayList<>();
        columns.addAll(Arrays.asList(this.columns));
        columns.addAll(Arrays.asList(o.columns));
        return of(columns.toArray(new Column[0]));
    }

    public boolean contains(String columnName) {
        return Arrays.stream(columns).anyMatch(i -> i.getName().equals(columnName));
    }

    public int size() {
        return columns.length;
    }

    public List<String> getAliasList() {
        return Arrays.asList(columns).stream().map(i -> i.getName()).collect(Collectors.toList());
    }
}